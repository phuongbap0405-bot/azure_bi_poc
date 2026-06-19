/**
 * Bronze Azure Function
 *
 * Flow:
 * ADF/manual HTTP POST -> BatchLog idempotency -> Checkpoint read ->
 * Controlio /api/v1/statistics/timeline pagination -> ADLS Bronze JSON ->
 * BatchLog Succeeded.
 *
 * Important:
 * - This Function never updates Checkpoint.
 * - Checkpoint is updated only after Gold commits successfully.
 * - The Controlio token is read from Key Vault through Managed Identity.
 * - ADLS and Azure Table Storage are accessed through Managed Identity.
 */

"use strict";

const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");
const { BlobServiceClient } = require("@azure/storage-blob");
const { TableClient } = require("@azure/data-tables");

// Reuse credentials and SDK clients across invocations in the same worker.
const credential = new DefaultAzureCredential();
let clientCache = null;

const DEFAULT_SOURCE_NAME = "controlio";
const DEFAULT_PIPELINE_TYPE = "raw";
const DEFAULT_ENTITY_NAME = "timeline";

const TRANSIENT_HTTP_STATUS_CODES = new Set([429, 500, 502, 503, 504]);
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const SAFE_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;

function createAppError(message, options = {}) {
    const error = new Error(message);
    error.errorCode = options.errorCode ?? "UNKNOWN";
    error.errorCategory = options.errorCategory ?? "Permanent";
    error.isPermanent = options.isPermanent ?? true;
    error.httpStatus = options.httpStatus;
    return error;
}

function sanitizeTableKey(value) {
    return String(value)
        .replace(/[\/\\#?]/g, "-")
        .replace(/[\u0000-\u001F\u007F]/g, "-");
}

function sanitizeBlobPathSegment(value) {
    return encodeURIComponent(String(value));
}

function validateLogicalName(value, fieldName) {
    if (!value || !SAFE_NAME_PATTERN.test(value)) {
        throw createAppError(
            `${fieldName} must contain only letters, numbers, underscores, or hyphens`,
            {
                errorCode: "INVALID_LOGICAL_NAME",
                errorCategory: "DataQuality",
                httpStatus: 400,
            }
        );
    }
}

function parsePositiveInteger(value, fallback, fieldName, maximum = Number.MAX_SAFE_INTEGER) {
    const parsed = Number.parseInt(value ?? String(fallback), 10);

    if (!Number.isInteger(parsed) || parsed <= 0 || parsed > maximum) {
        throw createAppError(`${fieldName} must be an integer between 1 and ${maximum}`, {
            errorCode: "INVALID_CONFIGURATION",
            errorCategory: "Configuration",
            httpStatus: 500,
        });
    }

    return parsed;
}

function parseOptionalUtcDate(value, fieldName) {
    if (value === undefined || value === null || value === "") {
        return undefined;
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        throw createAppError(`${fieldName} must be a valid UTC date-time`, {
            errorCode: "INVALID_DATETIME",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return date;
}

function validateDateRange(startTime, endTime) {
    if (!startTime || !endTime) {
        throw createAppError("start_time and end_time are required", {
            errorCode: "MISSING_DATE_RANGE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    if (!DATE_PATTERN.test(startTime) || !DATE_PATTERN.test(endTime)) {
        throw createAppError("start_time and end_time must use YYYY-MM-DD format", {
            errorCode: "INVALID_DATE_FORMAT",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const start = new Date(`${startTime}T00:00:00.000Z`);
    const end = new Date(`${endTime}T00:00:00.000Z`);

    // This also rejects impossible dates such as 2026-02-31.
    if (
        Number.isNaN(start.getTime()) ||
        Number.isNaN(end.getTime()) ||
        start.toISOString().slice(0, 10) !== startTime ||
        end.toISOString().slice(0, 10) !== endTime
    ) {
        throw createAppError("start_time or end_time is not a valid calendar date", {
            errorCode: "INVALID_DATE_VALUE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    if (start > end) {
        throw createAppError("start_time cannot be later than end_time", {
            errorCode: "INVALID_DATE_RANGE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }
}

function normalizeCommaSeparatedIds(value, fieldName) {
    if (value === undefined || value === null || value === "") {
        return null;
    }

    const normalized = String(value)
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);

    if (normalized.length === 0 || normalized.some((item) => !/^\d+$/.test(item))) {
        throw createAppError(`${fieldName} must be a comma-separated list of integer IDs`, {
            errorCode: "INVALID_ID_FILTER",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return normalized.join(",");
}

function normalizeActivityType(value) {
    if (value === undefined || value === null || value === "") {
        return null;
    }

    const parsed = Number(value);
    if (![0, 1, 2].includes(parsed)) {
        throw createAppError("activity_type must be 0, 1, or 2", {
            errorCode: "INVALID_ACTIVITY_TYPE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return parsed;
}

function compactUtcTimestamp(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        throw createAppError("trigger_time_utc must be a valid UTC date-time", {
            errorCode: "INVALID_TRIGGER_TIME",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return date
        .toISOString()
        .replace(/[-:]/g, "")
        .replace(/\.\d{3}Z$/, "Z");
}

function generateBatchId(pipelineType, entityName, triggerTimeUtc, runId) {
    const compact = compactUtcTimestamp(triggerTimeUtc);
    const shortRunId = String(runId)
        .replace(/[^A-Za-z0-9]/g, "")
        .slice(0, 8)
        .toLowerCase();

    if (!shortRunId) {
        throw createAppError("adf_pipeline_run_id does not contain usable characters", {
            errorCode: "INVALID_RUN_ID",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return sanitizeTableKey(`${pipelineType}_${entityName}_${compact}_${shortRunId}`);
}

function extractBatchMonth(batchId, fallbackTriggerTimeUtc) {
    const match = String(batchId).match(/_(\d{6})\d{2}T\d{6}Z_/);
    if (match) {
        return match[1];
    }

    const fallbackDate = new Date(fallbackTriggerTimeUtc);
    if (Number.isNaN(fallbackDate.getTime())) {
        throw createAppError("Cannot determine BatchLog partition month", {
            errorCode: "INVALID_BATCH_PARTITION_MONTH",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return fallbackDate.toISOString().slice(0, 7).replace("-", "");
}

function buildBatchLogPartitionKey(sourceName, pipelineType, entityName, batchMonth) {
    return sanitizeTableKey(`${sourceName}|${pipelineType}|${entityName}|${batchMonth}`);
}

function buildCheckpointPartitionKey(sourceName, pipelineType) {
    return sanitizeTableKey(`${sourceName}|${pipelineType}`);
}

/**
 * Controlio documents prev as: start_time,user_id,computer_id.
 * This helper supports the common flat and nested forms. If the real response
 * uses different field names, it fails clearly instead of inventing a cursor.
 */
function buildCursorFromLastRow(rows) {
    if (!Array.isArray(rows) || rows.length === 0) {
        return null;
    }

    const last = rows[rows.length - 1];
    const cursorStart = last.start_time ?? last.start;
    const userId = last.user_id ?? last.userId ?? last.user?.id;
    const computerId = last.computer_id ?? last.computerId ?? last.computer?.id;

    if (
        cursorStart === undefined ||
        cursorStart === null ||
        userId === undefined ||
        userId === null ||
        computerId === undefined ||
        computerId === null
    ) {
        throw createAppError(
            `Cannot build prev cursor from the final record. Available fields: ${Object.keys(last).join(", ")}`,
            {
                errorCode: "CURSOR_FIELDS_MISSING",
                errorCategory: "DataQuality",
                httpStatus: 500,
            }
        );
    }

    return `${cursorStart},${userId},${computerId}`;
}

async function getTableEntity(tableClient, partitionKey, rowKey) {
    try {
        return await tableClient.getEntity(partitionKey, rowKey);
    } catch (error) {
        if (error.statusCode === 404) {
            return null;
        }
        throw error;
    }
}

function sleep(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function getRetryAfterMilliseconds(response) {
    const value = response.headers.get("retry-after");
    if (!value) {
        return null;
    }

    const seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) {
        return seconds * 1000;
    }

    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
        return Math.max(0, date.getTime() - Date.now());
    }

    return null;
}

async function readResponseTextSafely(response) {
    try {
        return await response.text();
    } catch {
        return "";
    }
}

async function fetchControlioPageWithRetry({
    url,
    bearerToken,
    context,
    maxAttempts,
    timeoutMs,
}) {
    let lastError;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        const abortController = new AbortController();
        const timeoutHandle = setTimeout(() => abortController.abort(), timeoutMs);

        try {
            const response = await fetch(url, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${bearerToken}`,
                    Accept: "application/json",
                },
                signal: abortController.signal,
            });

            if (!response.ok) {
                const responseText = await readResponseTextSafely(response);
                const isTransient = TRANSIENT_HTTP_STATUS_CODES.has(response.status);
                const error = createAppError(
                    `Controlio API returned HTTP ${response.status}: ${responseText.slice(0, 300)}`,
                    {
                        errorCode: `CONTROLIO_${response.status}`,
                        errorCategory:
                            response.status === 401 || response.status === 403
                                ? "Configuration"
                                : isTransient
                                    ? "Transient"
                                    : "Permanent",
                        isPermanent: !isTransient,
                        httpStatus: isTransient ? 503 : 500,
                    }
                );

                error.retryAfterMs = response.status === 429
                    ? getRetryAfterMilliseconds(response)
                    : null;

                throw error;
            }

            let rawResponse;
            try {
                rawResponse = await response.json();
            } catch (parseError) {
                throw createAppError(`Controlio returned invalid JSON: ${parseError.message}`, {
                    errorCode: "CONTROLIO_INVALID_JSON",
                    errorCategory: "Permanent",
                    isPermanent: true,
                    httpStatus: 500,
                });
            }

            if (!rawResponse || typeof rawResponse !== "object" || !Array.isArray(rawResponse.data)) {
                throw createAppError("Controlio response must contain a data array", {
                    errorCode: "CONTROLIO_INVALID_RESPONSE",
                    errorCategory: "Permanent",
                    isPermanent: true,
                    httpStatus: 500,
                });
            }

            return {
                rows: rawResponse.data,
                rawResponse,
                attemptCount: attempt,
            };
        } catch (error) {
            const isTimeout = error.name === "AbortError";
            const normalizedError = isTimeout
                ? createAppError(`Controlio request timed out after ${timeoutMs} ms`, {
                    errorCode: "CONTROLIO_TIMEOUT",
                    errorCategory: "Transient",
                    isPermanent: false,
                    httpStatus: 503,
                })
                : error;

            if (
                normalizedError.isPermanent === undefined &&
                !(normalizedError.errorCode || normalizedError.errorCategory)
            ) {
                normalizedError.errorCode = "CONTROLIO_NETWORK_ERROR";
                normalizedError.errorCategory = "Transient";
                normalizedError.isPermanent = false;
                normalizedError.httpStatus = 503;
            }

            if (normalizedError.isPermanent) {
                throw normalizedError;
            }

            lastError = normalizedError;

            if (attempt < maxAttempts) {
                const exponentialBackoffMs = 1000 * 2 ** (attempt - 1);
                const retryDelayMs = Math.min(
                    normalizedError.retryAfterMs ?? exponentialBackoffMs,
                    60_000
                );

                context.warn(
                    `Controlio request attempt ${attempt}/${maxAttempts} failed ` +
                    `(${normalizedError.errorCode ?? "UNKNOWN"}). Retrying in ${retryDelayMs} ms.`
                );

                await sleep(retryDelayMs);
            }
        } finally {
            clearTimeout(timeoutHandle);
        }
    }

    const exhaustedError = lastError ?? createAppError("Controlio retry attempts were exhausted", {
        errorCode: "CONTROLIO_RETRY_EXHAUSTED",
        errorCategory: "Transient",
        isPermanent: false,
        httpStatus: 503,
    });

    exhaustedError.errorCode = exhaustedError.errorCode ?? "CONTROLIO_RETRY_EXHAUSTED";
    exhaustedError.errorCategory = exhaustedError.errorCategory ?? "Transient";
    exhaustedError.isPermanent = false;
    exhaustedError.httpStatus = 503;
    throw exhaustedError;
}

function getAzureClients(config) {
    const cacheKey = [
        config.storageAccount,
        config.batchLogTable,
        config.checkpointTable,
        config.keyVaultUrl,
    ].join("|");

    if (clientCache?.cacheKey === cacheKey) {
        return clientCache.clients;
    }

    const tableServiceUrl = `https://${config.storageAccount}.table.core.windows.net`;
    const blobServiceUrl = `https://${config.storageAccount}.blob.core.windows.net`;

    const clients = {
        batchLogClient: new TableClient(
            tableServiceUrl,
            config.batchLogTable,
            credential
        ),
        checkpointClient: new TableClient(
            tableServiceUrl,
            config.checkpointTable,
            credential
        ),
        blobServiceClient: new BlobServiceClient(blobServiceUrl, credential),
        secretClient: new SecretClient(config.keyVaultUrl, credential),
    };

    clientCache = { cacheKey, clients };
    return clients;
}

function getHttpStatusForError(error) {
    if (Number.isInteger(error.httpStatus)) {
        return error.httpStatus;
    }

    return error.errorCategory === "Transient" ? 503 : 500;
}

app.http("Bronze", {
    methods: ["POST"],
    authLevel: "function",

    handler: async (request, context) => {
        let batchId;
        let batchLogPartitionKey;
        let batchLogClient;

        try {
            let body;
            try {
                body = await request.json();
            } catch (error) {
                return {
                    status: 400,
                    jsonBody: {
                        success: false,
                        error_code: "INVALID_JSON_BODY",
                        message: `Request body must be valid JSON: ${error.message}`,
                    },
                };
            }

            const runId = body.adf_pipeline_run_id;
            if (!runId) {
                return {
                    status: 400,
                    jsonBody: {
                        success: false,
                        error_code: "MISSING_RUN_ID",
                        message: "adf_pipeline_run_id is required",
                    },
                };
            }

            const windowStartUtcText = body.window_start_utc ?? null;
            const windowEndUtcText = body.window_end_utc ?? null;
            const startTime = body.start_time ?? windowStartUtcText?.slice(0, 10) ?? null;
            const endTime = body.end_time ?? windowEndUtcText?.slice(0, 10) ?? null;
            validateDateRange(startTime, endTime);

            const sourceName = body.source_name ?? DEFAULT_SOURCE_NAME;
            const pipelineType = body.pipeline_type ?? DEFAULT_PIPELINE_TYPE;
            const entityName =
                body.entity_name ??
                process.env.CONTROLIO_ENTITY_NAME ??
                DEFAULT_ENTITY_NAME;

            validateLogicalName(sourceName, "source_name");
            validateLogicalName(pipelineType, "pipeline_type");
            validateLogicalName(entityName, "entity_name");

            const triggerTimeUtcText =
                body.trigger_time_utc ??
                windowEndUtcText ??
                new Date().toISOString();

            const triggerTimeUtc = parseOptionalUtcDate(triggerTimeUtcText, "trigger_time_utc");
            const windowStartUtc = parseOptionalUtcDate(windowStartUtcText, "window_start_utc");
            const windowEndUtc = parseOptionalUtcDate(windowEndUtcText, "window_end_utc");

            const activities = normalizeCommaSeparatedIds(body.activities, "activities");
            const users = normalizeCommaSeparatedIds(body.users, "users");
            const departments = normalizeCommaSeparatedIds(body.departments, "departments");
            const activityType = normalizeActivityType(body.activity_type);
            const stringToMatch =
                body.string_to_match === undefined || body.string_to_match === null
                    ? null
                    : String(body.string_to_match);

            const storageAccount = process.env.DATA_STORAGE_ACCOUNT;
            const bronzeContainer = process.env.BRONZE_CONTAINER;
            const batchLogTable = process.env.BATCH_LOG_TABLE;
            const checkpointTable = process.env.CHECKPOINT_TABLE;
            const keyVaultUrl = process.env.KEY_VAULT_URL;
            const tokenSecretName = process.env.CONTROLIO_TOKEN_SECRET_NAME;
            const controlioBaseUrl = process.env.CONTROLIO_BASE_URL;

            const missingVariables = [
                ["DATA_STORAGE_ACCOUNT", storageAccount],
                ["BRONZE_CONTAINER", bronzeContainer],
                ["BATCH_LOG_TABLE", batchLogTable],
                ["CHECKPOINT_TABLE", checkpointTable],
                ["KEY_VAULT_URL", keyVaultUrl],
                ["CONTROLIO_TOKEN_SECRET_NAME", tokenSecretName],
                ["CONTROLIO_BASE_URL", controlioBaseUrl],
            ]
                .filter(([, value]) => !value)
                .map(([name]) => name);

            if (missingVariables.length > 0) {
                throw createAppError(
                    `Missing environment variables: ${missingVariables.join(", ")}`,
                    {
                        errorCode: "MISSING_ENV_VARS",
                        errorCategory: "Configuration",
                        httpStatus: 500,
                    }
                );
            }

            const pageLimit = parsePositiveInteger(
                process.env.CONTROLIO_PAGE_LIMIT,
                10_000,
                "CONTROLIO_PAGE_LIMIT",
                10_000
            );
            const maxPages = parsePositiveInteger(
                process.env.MAX_PAGES_PER_BATCH,
                100,
                "MAX_PAGES_PER_BATCH",
                100_000
            );
            const maxApiAttempts = parsePositiveInteger(
                process.env.CONTROLIO_MAX_API_ATTEMPTS,
                3,
                "CONTROLIO_MAX_API_ATTEMPTS",
                10
            );
            const requestTimeoutMs = parsePositiveInteger(
                process.env.CONTROLIO_REQUEST_TIMEOUT_MS,
                60_000,
                "CONTROLIO_REQUEST_TIMEOUT_MS",
                220_000
            );

            const resumeBatchId = body.resume_batch_id || null;
            batchId = resumeBatchId
                ? sanitizeTableKey(resumeBatchId)
                : generateBatchId(pipelineType, entityName, triggerTimeUtcText, runId);

            const batchMonth = extractBatchMonth(batchId, triggerTimeUtcText);
            batchLogPartitionKey = buildBatchLogPartitionKey(
                sourceName,
                pipelineType,
                entityName,
                batchMonth
            );

            const clients = getAzureClients({
                storageAccount,
                batchLogTable,
                checkpointTable,
                keyVaultUrl,
            });

            batchLogClient = clients.batchLogClient;
            const checkpointClient = clients.checkpointClient;
            const containerClient = clients.blobServiceClient.getContainerClient(bronzeContainer);
            const secretClient = clients.secretClient;

            context.log(
                `Bronze started: batch_id=${batchId}, partition_key=${batchLogPartitionKey}`
            );

            const existingBatch = await getTableEntity(
                batchLogClient,
                batchLogPartitionKey,
                batchId
            );

            if (existingBatch?.bronze_status === "Succeeded") {
                return {
                    status: 200,
                    jsonBody: {
                        success: true,
                        skipped: true,
                        batch_id: batchId,
                        bronze_path: existingBatch.bronze_path || null,
                        page_count: existingBatch.page_count || 0,
                        raw_row_count: existingBatch.raw_row_count || 0,
                        output_cursor_prev: existingBatch.output_cursor_prev || null,
                        message: "Bronze already succeeded; processing was skipped idempotently",
                    },
                };
            }

            const now = new Date();
            const isNewPipelineRun =
                existingBatch && existingBatch.current_adf_pipeline_run_id !== runId;
            const runMode =
                body.run_mode ??
                (resumeBatchId || isNewPipelineRun
                    ? "Resume"
                    : body.trigger_type === "Manual"
                        ? "Manual"
                        : "Scheduled");

            if (!existingBatch) {
                const newBatchEntity = {
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    batch_id: batchId,
                    initial_adf_pipeline_run_id: String(runId),
                    current_adf_pipeline_run_id: String(runId),
                    adf_pipeline_name: String(
                        body.adf_pipeline_name ?? "pl_controlio_timeline"
                    ),
                    trigger_type: String(body.trigger_type ?? "Manual"),
                    trigger_time_utc: triggerTimeUtc,
                    run_mode: runMode,
                    rerun_count: 0,
                    pipeline_type: pipelineType,
                    source_name: sourceName,
                    entity_name: entityName,
                    start_time: startTime,
                    end_time: endTime,
                    batch_status: "Created",
                    current_stage: "Bronze",
                    bronze_status: "NotStarted",
                    silver_status: "NotStarted",
                    gold_status: "NotStarted",
                    checkpoint_status: "NotStarted",
                    api_call_count: 0,
                    page_count: 0,
                    raw_row_count: 0,
                    clean_row_count: 0,
                    rejected_row_count: 0,
                    insert_count: 0,
                    update_count: 0,
                    skip_count: 0,
                    bronze_pagination_complete: false,
                    schema_version: process.env.SCHEMA_VERSION ?? "1.0",
                    code_version: process.env.CODE_VERSION ?? "local",
                    created_at: now,
                    updated_at: now,
                };

                if (windowStartUtc) {
                    newBatchEntity.window_start_utc = windowStartUtc;
                }
                if (windowEndUtc) {
                    newBatchEntity.window_end_utc = windowEndUtc;
                }

                await batchLogClient.upsertEntity(newBatchEntity, "Merge");
            } else {
                await batchLogClient.upsertEntity(
                    {
                        partitionKey: batchLogPartitionKey,
                        rowKey: batchId,
                        current_adf_pipeline_run_id: String(runId),
                        run_mode: runMode,
                        rerun_count:
                            Number(existingBatch.rerun_count ?? 0) +
                            (isNewPipelineRun ? 1 : 0),
                        updated_at: now,
                    },
                    "Merge"
                );
            }

            const checkpointPartitionKey = buildCheckpointPartitionKey(
                sourceName,
                pipelineType
            );
            const checkpointEntity = await getTableEntity(
                checkpointClient,
                checkpointPartitionKey,
                entityName
            );

            /*
             * For the aggregated /statistics/timeline endpoint, prev is used to
             * paginate one date-range query. It is not reused across new scheduled
             * batches. For a raw pipeline, a committed cursor may be reused.
             */
            const checkpointCursor = checkpointEntity?.committed_cursor_prev || null;
            const originalInputCursor =
                existingBatch?.input_cursor_prev ||
                (pipelineType === "raw" ? checkpointCursor : null);

            const resumeCursor =
                existingBatch?.last_persisted_cursor_prev || originalInputCursor;

            const startedAt = existingBatch?.started_at ?? now;
            const bronzeStartedAt = existingBatch?.bronze_started_at ?? now;

            await batchLogClient.upsertEntity(
                {
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    input_cursor_prev: originalInputCursor ?? "",
                    batch_status: "Running",
                    current_stage: "Bronze",
                    bronze_status: "Running",
                    current_adf_pipeline_run_id: String(runId),
                    started_at: startedAt,
                    bronze_started_at: bronzeStartedAt,
                    updated_at: new Date(),
                    failed_stage: "",
                    error_category: "",
                    error_code: "",
                    error_message: "",
                },
                "Merge"
            );

            const tokenSecret = await secretClient.getSecret(tokenSecretName);
            if (!tokenSecret.value) {
                throw createAppError("Controlio API token in Key Vault is empty", {
                    errorCode: "EMPTY_API_TOKEN",
                    errorCategory: "Configuration",
                    httpStatus: 500,
                });
            }

            const blobPrefix = [
                sanitizeBlobPathSegment(sourceName),
                sanitizeBlobPathSegment(entityName),
                `start_date=${sanitizeBlobPathSegment(startTime)}`,
                `end_date=${sanitizeBlobPathSegment(endTime)}`,
                `batch_id=${sanitizeBlobPathSegment(batchId)}`,
            ].join("/");
            const bronzePath = `${bronzeContainer}/${blobPrefix}`;

            let currentPrev = resumeCursor;
            let lastPersistedCursor =
                existingBatch?.last_persisted_cursor_prev || originalInputCursor;
            let pageNumber = Number(existingBatch?.page_count ?? 0);
            let totalRows = Number(existingBatch?.raw_row_count ?? 0);
            let totalApiCalls = Number(existingBatch?.api_call_count ?? 0);
            let paginationComplete = Boolean(existingBatch?.bronze_pagination_complete);

            const seenCursors = new Set();
            if (currentPrev) {
                seenCursors.add(currentPrev);
            }

            while (!paginationComplete) {
                if (pageNumber >= maxPages) {
                    throw createAppError(
                        "Maximum page limit reached before pagination completed",
                        {
                            errorCode: "MAX_PAGES_REACHED",
                            errorCategory: "Permanent",
                            httpStatus: 500,
                        }
                    );
                }

                const nextPageNumber = pageNumber + 1;
                const apiUrl = new URL(
                    "/api/v1/statistics/timeline",
                    controlioBaseUrl.endsWith("/")
                        ? controlioBaseUrl
                        : `${controlioBaseUrl}/`
                );

                apiUrl.searchParams.set("start_time", startTime);
                apiUrl.searchParams.set("end_time", endTime);
                apiUrl.searchParams.set("limit", String(pageLimit));
                apiUrl.searchParams.set("sort_direction", "asc");

                if (currentPrev) {
                    apiUrl.searchParams.set("prev", currentPrev);
                }
                if (activities) {
                    apiUrl.searchParams.set("activities", activities);
                }
                if (users) {
                    apiUrl.searchParams.set("users", users);
                }
                if (departments) {
                    apiUrl.searchParams.set("departments", departments);
                }
                if (stringToMatch) {
                    apiUrl.searchParams.set("string_to_match", stringToMatch);
                }
                if (activityType !== null) {
                    apiUrl.searchParams.set("activity_type", String(activityType));
                }

                context.log(
                    `Bronze fetching page ${nextPageNumber} for batch_id=${batchId}`
                );

                const { rows, rawResponse, attemptCount } =
                    await fetchControlioPageWithRetry({
                        url: apiUrl.toString(),
                        bearerToken: tokenSecret.value,
                        context,
                        maxAttempts: maxApiAttempts,
                        timeoutMs: requestTimeoutMs,
                    });

                totalApiCalls += attemptCount;

                const pageOutputCursor = buildCursorFromLastRow(rows);
                const nextPrev = rows.length === pageLimit ? pageOutputCursor : null;

                if (nextPrev && seenCursors.has(nextPrev)) {
                    throw createAppError(`Repeated pagination cursor detected: ${nextPrev}`, {
                        errorCode: "REPEATED_CURSOR",
                        errorCategory: "Permanent",
                        httpStatus: 500,
                    });
                }

                const requestParameters = {
                    start_time: startTime,
                    end_time: endTime,
                    limit: pageLimit,
                    sort_direction: "asc",
                    prev: currentPrev ?? null,
                    activities,
                    users,
                    departments,
                    string_to_match: stringToMatch,
                    activity_type: activityType,
                };

                const pagePath = `${blobPrefix}/page-${String(nextPageNumber).padStart(4, "0")}.json`;
                const pageContent = JSON.stringify(
                    {
                        batch_id: batchId,
                        page_number: nextPageNumber,
                        input_prev: currentPrev ?? null,
                        output_prev: pageOutputCursor,
                        next_request_prev: nextPrev,
                        row_count: rows.length,
                        fetched_at_utc: new Date().toISOString(),
                        request_parameters: requestParameters,
                        response: rawResponse,
                    },
                    null,
                    2
                );

                await containerClient
                    .getBlockBlobClient(pagePath)
                    .upload(pageContent, Buffer.byteLength(pageContent), {
                        blobHTTPHeaders: {
                            blobContentType: "application/json",
                        },
                    });

                pageNumber = nextPageNumber;
                totalRows += rows.length;
                lastPersistedCursor = pageOutputCursor ?? currentPrev;
                paginationComplete = nextPrev === null;

                await batchLogClient.upsertEntity(
                    {
                        partitionKey: batchLogPartitionKey,
                        rowKey: batchId,
                        page_count: pageNumber,
                        raw_row_count: totalRows,
                        api_call_count: totalApiCalls,
                        last_persisted_cursor_prev: lastPersistedCursor ?? "",
                        bronze_pagination_complete: paginationComplete,
                        updated_at: new Date(),
                    },
                    "Merge"
                );

                if (nextPrev) {
                    seenCursors.add(nextPrev);
                    currentPrev = nextPrev;
                }
            }

            const outputCursorPrev = lastPersistedCursor;
            const manifestPath = `${blobPrefix}/manifest.json`;
            const manifestContent = JSON.stringify(
                {
                    batch_id: batchId,
                    source_name: sourceName,
                    entity_name: entityName,
                    pipeline_type: pipelineType,
                    adf_pipeline_run_id: String(runId),
                    start_time: startTime,
                    end_time: endTime,
                    window_start_utc: windowStartUtcText,
                    window_end_utc: windowEndUtcText,
                    input_cursor_prev: originalInputCursor,
                    output_cursor_prev: outputCursorPrev,
                    page_count: pageNumber,
                    raw_row_count: totalRows,
                    api_call_count: totalApiCalls,
                    bronze_path: bronzePath,
                    status: "Succeeded",
                    completed_at_utc: new Date().toISOString(),
                },
                null,
                2
            );

            await containerClient
                .getBlockBlobClient(manifestPath)
                .upload(manifestContent, Buffer.byteLength(manifestContent), {
                    blobHTTPHeaders: {
                        blobContentType: "application/json",
                    },
                });

            const bronzeFinishedAt = new Date();
            await batchLogClient.upsertEntity(
                {
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    output_cursor_prev: outputCursorPrev ?? "",
                    bronze_path: bronzePath,
                    bronze_status: "Succeeded",
                    current_stage: "Silver",
                    page_count: pageNumber,
                    raw_row_count: totalRows,
                    api_call_count: totalApiCalls,
                    bronze_pagination_complete: true,
                    bronze_finished_at: bronzeFinishedAt,
                    updated_at: bronzeFinishedAt,
                    failed_stage: "",
                    error_category: "",
                    error_code: "",
                    error_message: "",
                },
                "Merge"
            );

            context.log(
                `Bronze succeeded: batch_id=${batchId}, pages=${pageNumber}, rows=${totalRows}`
            );

            return {
                status: 200,
                jsonBody: {
                    success: true,
                    batch_id: batchId,
                    source_name: sourceName,
                    entity_name: entityName,
                    pipeline_type: pipelineType,
                    start_time: startTime,
                    end_time: endTime,
                    input_cursor_prev: originalInputCursor,
                    output_cursor_prev: outputCursorPrev,
                    bronze_path: bronzePath,
                    page_count: pageNumber,
                    raw_row_count: totalRows,
                    api_call_count: totalApiCalls,
                },
            };
        } catch (error) {
            context.error("Bronze Function failed", {
                batch_id: batchId,
                error_code: error.errorCode,
                error_category: error.errorCategory,
                message: error.message,
            });

            if (batchLogClient && batchLogPartitionKey && batchId) {
                try {
                    const failedAt = new Date();
                    await batchLogClient.upsertEntity(
                        {
                            partitionKey: batchLogPartitionKey,
                            rowKey: batchId,
                            batch_status: "Failed",
                            current_stage: "Bronze",
                            bronze_status: "Failed",
                            failed_stage: "Bronze",
                            error_category: error.errorCategory ?? "Transient",
                            error_code: error.errorCode ?? "UNKNOWN",
                            error_message: String(error.message).slice(0, 1000),
                            error_details_ref: context.invocationId ?? "",
                            failed_at: failedAt,
                            updated_at: failedAt,
                        },
                        "Merge"
                    );
                } catch (logError) {
                    context.error("Bronze failed to update BatchLog failure state", {
                        message: logError.message,
                    });
                }
            }

            return {
                status: getHttpStatusForError(error),
                jsonBody: {
                    success: false,
                    batch_id: batchId ?? null,
                    error_code: error.errorCode ?? "UNKNOWN",
                    error_category: error.errorCategory ?? "Transient",
                    message: error.message,
                },
            };
        }
    },
});