/**
 * Silver Azure Function
 *
 * Flow:
 * HTTP POST -> read Bronze manifest/pages -> validate/normalize/deduplicate ->
 * write clean JSONL files to ADLS Silver -> write Silver manifest -> update BatchLog.
 *
 * Notes:
 * - Bronze remains the complete raw recovery layer.
 * - Silver intentionally excludes full URL, user_name, user_friendly_name,
 *   computer_name, and data from the clean fact output.
 * - Controlio timeline timestamps are normalized to UTC ISO-8601.
 */

"use strict";

const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");
const { TableClient } = require("@azure/data-tables");
const crypto = require("node:crypto");

const credential = new DefaultAzureCredential();
let clientCache = null;

const SAFE_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

function createAppError(message, options = {}) {
    const error = new Error(message);
    error.errorCode = options.errorCode ?? "UNKNOWN";
    error.errorCategory = options.errorCategory ?? "Permanent";
    error.httpStatus = options.httpStatus ?? 500;
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

function parsePositiveInteger(value, fallback, fieldName, maximum) {
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

function normalizeBronzePrefix(bronzePath, bronzeContainer) {
    let value = String(bronzePath ?? "").trim().replace(/^\/+|\/+$/g, "");
    if (!value) {
        throw createAppError("bronze_path is required", {
            errorCode: "MISSING_BRONZE_PATH",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const containerPrefix = `${bronzeContainer}/`;
    if (value.toLowerCase().startsWith(containerPrefix.toLowerCase())) {
        value = value.slice(containerPrefix.length);
    }

    if (!value || value.includes("..")) {
        throw createAppError("bronze_path is invalid", {
            errorCode: "INVALID_BRONZE_PATH",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return value;
}

function extractBatchMonth(batchId, fallbackDate) {
    const match = String(batchId).match(/_(\d{6})\d{2}T\d{6}Z_/);
    if (match) {
        return match[1];
    }

    const date = new Date(fallbackDate);
    if (Number.isNaN(date.getTime())) {
        throw createAppError("Cannot determine BatchLog partition month", {
            errorCode: "INVALID_BATCH_PARTITION_MONTH",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return date.toISOString().slice(0, 7).replace("-", "");
}

function buildBatchLogPartitionKey(sourceName, pipelineType, entityName, batchMonth) {
    return sanitizeTableKey(`${sourceName}|${pipelineType}|${entityName}|${batchMonth}`);
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

async function downloadBlobText(containerClient, blobPath) {
    const blobClient = containerClient.getBlobClient(blobPath);
    const exists = await blobClient.exists();
    if (!exists) {
        throw createAppError(`Blob not found: ${blobPath}`, {
            errorCode: "BLOB_NOT_FOUND",
            errorCategory: "DataQuality",
            httpStatus: 404,
        });
    }

    const response = await blobClient.download();
    if (!response.readableStreamBody) {
        throw createAppError(`Blob has no readable content: ${blobPath}`, {
            errorCode: "EMPTY_BLOB_STREAM",
            errorCategory: "Transient",
            httpStatus: 503,
        });
    }

    const chunks = [];
    for await (const chunk of response.readableStreamBody) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString("utf8");
}

async function downloadJson(containerClient, blobPath) {
    const text = await downloadBlobText(containerClient, blobPath);
    try {
        return JSON.parse(text);
    } catch (error) {
        throw createAppError(`Invalid JSON in ${blobPath}: ${error.message}`, {
            errorCode: "INVALID_BRONZE_JSON",
            errorCategory: "DataQuality",
            httpStatus: 500,
        });
    }
}

function normalizeControlioUtcTimestamp(value, fieldName) {
    if (typeof value !== "string" || !value.trim()) {
        throw createAppError(`${fieldName} is required`, {
            errorCode: "MISSING_TIMESTAMP",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    let normalized = value.trim();

    // JavaScript Date supports milliseconds. Controlio can return microseconds,
    // so keep the first three fractional digits for parsing.
    normalized = normalized.replace(/\.(\d{3})\d+/, ".$1");

    // Controlio response timestamps are UTC. Add Z when the source omits an offset.
    if (!/[zZ]$|[+-]\d{2}:?\d{2}$/.test(normalized)) {
        normalized += "Z";
    }

    const date = new Date(normalized);
    if (Number.isNaN(date.getTime())) {
        throw createAppError(`${fieldName} is not a valid timestamp: ${value}`, {
            errorCode: "INVALID_TIMESTAMP",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return date.toISOString();
}

function toInteger(value, fieldName) {
    const parsed = Number(value);
    if (!Number.isInteger(parsed)) {
        throw createAppError(`${fieldName} must be an integer`, {
            errorCode: "INVALID_INTEGER",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }
    return parsed;
}

function toBoolean(value, fallback = false) {
    if (typeof value === "boolean") {
        return value;
    }
    if (value === 1 || value === "1" || value === "true") {
        return true;
    }
    if (value === 0 || value === "0" || value === "false") {
        return false;
    }
    return fallback;
}

function nullableString(value) {
    if (value === undefined || value === null) {
        return null;
    }
    const normalized = String(value).trim();
    return normalized === "" ? null : normalized;
}

function extractUrlDomain(value) {
    const text = nullableString(value);
    if (!text) {
        return null;
    }

    try {
        const candidate = /^[a-zA-Z][a-zA-Z\d+.-]*:\/\//.test(text)
            ? text
            : `https://${text}`;
        return new URL(candidate).hostname.toLowerCase() || null;
    } catch {
        return null;
    }
}

function createUniqueKey(rawRecord) {
    // Controlio defines start_time,user_id,computer_id as the composite unique
    // cursor for the timeline report. Preserve the source timestamp text here
    // so microsecond precision is not lost before hashing.
    const material = [
        String(rawRecord.start_time ?? ""),
        String(rawRecord.user_id ?? ""),
        String(rawRecord.computer_id ?? ""),
    ].join("|");

    return crypto.createHash("sha256").update(material).digest("hex");
}

function normalizeTimelineRecord(rawRecord, metadata) {
    if (!rawRecord || typeof rawRecord !== "object" || Array.isArray(rawRecord)) {
        throw createAppError("Timeline record must be an object", {
            errorCode: "INVALID_RECORD",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const userId = toInteger(rawRecord.user_id, "user_id");
    const computerId = toInteger(rawRecord.computer_id, "computer_id");
    const activityId = toInteger(rawRecord.activity_id, "activity_id");
    const activityType = toInteger(rawRecord.activity_type, "activity_type");
    const startTimeUtc = normalizeControlioUtcTimestamp(rawRecord.start_time, "start_time");
    const endTimeUtc = normalizeControlioUtcTimestamp(rawRecord.end_time, "end_time");

    const durationSeconds = Math.max(
        0,
        Math.round((new Date(endTimeUtc).getTime() - new Date(startTimeUtc).getTime()) / 1000)
    );

    return {
        unique_key: createUniqueKey(rawRecord),
        batch_id: metadata.batchId,
        source_page_number: metadata.pageNumber,
        source_name: metadata.sourceName,
        entity_name: metadata.entityName,
        activity: toBoolean(rawRecord.activity),
        activity_id: activityId,
        activity_type: activityType,
        activity_name: nullableString(rawRecord.activity_name),
        caption: nullableString(rawRecord.caption),
        user_id: userId,
        computer_id: computerId,
        start_time_utc: startTimeUtc,
        end_time_utc: endTimeUtc,
        activity_date: startTimeUtc.slice(0, 10),
        duration_seconds: durationSeconds,
        is_website: toBoolean(rawRecord.is_website),
        url_domain: extractUrlDomain(rawRecord.url),
        ingested_at_utc: metadata.ingestedAtUtc,
    };
}

function createRejectedRecord(rawRecord, metadata, reason) {
    return {
        batch_id: metadata.batchId,
        source_page_number: metadata.pageNumber,
        rejection_reason: String(reason).slice(0, 500),
        start_time: rawRecord?.start_time ?? null,
        end_time: rawRecord?.end_time ?? null,
        user_id: rawRecord?.user_id ?? null,
        computer_id: rawRecord?.computer_id ?? null,
        activity_id: rawRecord?.activity_id ?? null,
        rejected_at_utc: metadata.ingestedAtUtc,
    };
}

async function uploadJsonLines(containerClient, path, records) {
    if (records.length === 0) {
        return;
    }

    const content = `${records.map((item) => JSON.stringify(item)).join("\n")}\n`;
    await containerClient
        .getBlockBlobClient(path)
        .upload(content, Buffer.byteLength(content), {
            blobHTTPHeaders: {
                blobContentType: "application/x-ndjson",
            },
        });
}

async function uploadJson(containerClient, path, value) {
    const content = JSON.stringify(value, null, 2);
    await containerClient
        .getBlockBlobClient(path)
        .upload(content, Buffer.byteLength(content), {
            blobHTTPHeaders: {
                blobContentType: "application/json",
            },
        });
}

function getAzureClients(config) {
    const cacheKey = [
        config.storageAccount,
        config.batchLogTable,
    ].join("|");

    if (clientCache?.cacheKey === cacheKey) {
        return clientCache.clients;
    }

    const clients = {
        blobServiceClient: new BlobServiceClient(
            `https://${config.storageAccount}.blob.core.windows.net`,
            credential
        ),
        batchLogClient: new TableClient(
            `https://${config.storageAccount}.table.core.windows.net`,
            config.batchLogTable,
            credential
        ),
    };

    clientCache = { cacheKey, clients };
    return clients;
}

app.http("Silver", {
    methods: ["POST"],
    authLevel: "function",

    handler: async (request, context) => {
        let batchId = null;
        let batchLogPartitionKey = null;
        let batchLogClient = null;

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

            batchId = body.batch_id ? sanitizeTableKey(body.batch_id) : null;
            if (!batchId) {
                return {
                    status: 400,
                    jsonBody: {
                        success: false,
                        error_code: "MISSING_BATCH_ID",
                        message: "batch_id is required",
                    },
                };
            }

            const storageAccount = process.env.DATA_STORAGE_ACCOUNT;
            const bronzeContainer = process.env.BRONZE_CONTAINER;
            const silverContainer = process.env.SILVER_CONTAINER;
            const batchLogTable = process.env.BATCH_LOG_TABLE;

            const missingVariables = [
                ["DATA_STORAGE_ACCOUNT", storageAccount],
                ["BRONZE_CONTAINER", bronzeContainer],
                ["SILVER_CONTAINER", silverContainer],
                ["BATCH_LOG_TABLE", batchLogTable],
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

            const recordsPerFile = parsePositiveInteger(
                process.env.SILVER_RECORDS_PER_FILE,
                10_000,
                "SILVER_RECORDS_PER_FILE",
                100_000
            );

            const bronzePrefix = normalizeBronzePrefix(body.bronze_path, bronzeContainer);
            const clients = getAzureClients({ storageAccount, batchLogTable });
            batchLogClient = clients.batchLogClient;

            const bronzeContainerClient =
                clients.blobServiceClient.getContainerClient(bronzeContainer);
            const silverContainerClient =
                clients.blobServiceClient.getContainerClient(silverContainer);

            const manifestPath = `${bronzePrefix}/manifest.json`;
            const bronzeManifest = await downloadJson(bronzeContainerClient, manifestPath);

            if (String(bronzeManifest.batch_id) !== batchId) {
                throw createAppError(
                    `batch_id does not match Bronze manifest: request=${batchId}, manifest=${bronzeManifest.batch_id}`,
                    {
                        errorCode: "BATCH_ID_MISMATCH",
                        errorCategory: "DataQuality",
                        httpStatus: 400,
                    }
                );
            }

            if (bronzeManifest.status !== "Succeeded") {
                throw createAppError(
                    `Bronze manifest is not successful: ${bronzeManifest.status}`,
                    {
                        errorCode: "BRONZE_NOT_SUCCEEDED",
                        errorCategory: "DataQuality",
                        httpStatus: 409,
                    }
                );
            }

            const sourceName = bronzeManifest.source_name ?? "controlio";
            const pipelineType = bronzeManifest.pipeline_type ?? "raw";
            const entityName = bronzeManifest.entity_name ?? "timeline";
            validateLogicalName(sourceName, "source_name");
            validateLogicalName(pipelineType, "pipeline_type");
            validateLogicalName(entityName, "entity_name");

            const pageCount = Number(bronzeManifest.page_count ?? 0);
            if (!Number.isInteger(pageCount) || pageCount < 0) {
                throw createAppError("Bronze manifest page_count is invalid", {
                    errorCode: "INVALID_PAGE_COUNT",
                    errorCategory: "DataQuality",
                    httpStatus: 500,
                });
            }

            const startTime = String(bronzeManifest.start_time ?? "");
            const endTime = String(bronzeManifest.end_time ?? "");
            if (!DATE_PATTERN.test(startTime) || !DATE_PATTERN.test(endTime)) {
                throw createAppError("Bronze manifest start_time/end_time are invalid", {
                    errorCode: "INVALID_MANIFEST_DATE_RANGE",
                    errorCategory: "DataQuality",
                    httpStatus: 500,
                });
            }

            const fallbackDate =
                bronzeManifest.completed_at_utc ?? bronzeManifest.window_end_utc ?? new Date().toISOString();
            const batchMonth = extractBatchMonth(batchId, fallbackDate);
            batchLogPartitionKey = buildBatchLogPartitionKey(
                sourceName,
                pipelineType,
                entityName,
                batchMonth
            );

            const existingBatch = await getTableEntity(
                batchLogClient,
                batchLogPartitionKey,
                batchId
            );

            if (existingBatch?.silver_status === "Succeeded") {
                return {
                    status: 200,
                    jsonBody: {
                        success: true,
                        skipped: true,
                        batch_id: batchId,
                        silver_path: existingBatch.silver_path ?? null,
                        clean_row_count: existingBatch.clean_row_count ?? 0,
                        rejected_row_count: existingBatch.rejected_row_count ?? 0,
                        duplicate_row_count: existingBatch.duplicate_row_count ?? 0,
                        message: "Silver already succeeded; processing was skipped idempotently",
                    },
                };
            }

            const silverStartedAt = new Date();
            await batchLogClient.upsertEntity(
                {
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    batch_status: "Running",
                    current_stage: "Silver",
                    silver_status: "Running",
                    silver_started_at: silverStartedAt,
                    updated_at: silverStartedAt,
                    failed_stage: "",
                    error_category: "",
                    error_code: "",
                    error_message: "",
                },
                "Merge"
            );

            const silverPrefix = [
                sanitizeBlobPathSegment(sourceName),
                sanitizeBlobPathSegment(entityName),
                `start_date=${sanitizeBlobPathSegment(startTime)}`,
                `end_date=${sanitizeBlobPathSegment(endTime)}`,
                `batch_id=${sanitizeBlobPathSegment(batchId)}`,
            ].join("/");
            const silverPath = `${silverContainer}/${silverPrefix}`;

            const seenKeys = new Set();
            let cleanBuffer = [];
            let rejectedBuffer = [];
            let cleanPartNumber = 0;
            let rejectedPartNumber = 0;
            let cleanRowCount = 0;
            let rejectedRowCount = 0;
            let duplicateRowCount = 0;
            let sourceRowCount = 0;
            const ingestedAtUtc = new Date().toISOString();

            const flushClean = async () => {
                if (cleanBuffer.length === 0) {
                    return;
                }
                cleanPartNumber += 1;
                const path = `${silverPrefix}/part-${String(cleanPartNumber).padStart(4, "0")}.jsonl`;
                await uploadJsonLines(silverContainerClient, path, cleanBuffer);
                cleanBuffer = [];
            };

            const flushRejected = async () => {
                if (rejectedBuffer.length === 0) {
                    return;
                }
                rejectedPartNumber += 1;
                const path = `${silverPrefix}/rejected/part-${String(rejectedPartNumber).padStart(4, "0")}.jsonl`;
                await uploadJsonLines(silverContainerClient, path, rejectedBuffer);
                rejectedBuffer = [];
            };

            for (let pageNumber = 1; pageNumber <= pageCount; pageNumber += 1) {
                const pagePath = `${bronzePrefix}/page-${String(pageNumber).padStart(4, "0")}.json`;
                const page = await downloadJson(bronzeContainerClient, pagePath);
                const rows = page?.response?.data;

                if (!Array.isArray(rows)) {
                    throw createAppError(`${pagePath} does not contain response.data[]`, {
                        errorCode: "MISSING_RESPONSE_DATA",
                        errorCategory: "DataQuality",
                        httpStatus: 500,
                    });
                }

                sourceRowCount += rows.length;

                for (const rawRecord of rows) {
                    const metadata = {
                        batchId,
                        pageNumber,
                        sourceName,
                        entityName,
                        ingestedAtUtc,
                    };

                    try {
                        const normalized = normalizeTimelineRecord(rawRecord, metadata);

                        if (seenKeys.has(normalized.unique_key)) {
                            duplicateRowCount += 1;
                            continue;
                        }

                        seenKeys.add(normalized.unique_key);
                        cleanBuffer.push(normalized);
                        cleanRowCount += 1;

                        if (cleanBuffer.length >= recordsPerFile) {
                            await flushClean();
                        }
                    } catch (error) {
                        rejectedBuffer.push(
                            createRejectedRecord(rawRecord, metadata, error.message)
                        );
                        rejectedRowCount += 1;

                        if (rejectedBuffer.length >= recordsPerFile) {
                            await flushRejected();
                        }
                    }
                }
            }

            await flushClean();
            await flushRejected();

            const silverManifest = {
                batch_id: batchId,
                source_name: sourceName,
                entity_name: entityName,
                pipeline_type: pipelineType,
                start_time: startTime,
                end_time: endTime,
                bronze_path: `${bronzeContainer}/${bronzePrefix}`,
                silver_path: silverPath,
                source_page_count: pageCount,
                source_row_count: sourceRowCount,
                clean_row_count: cleanRowCount,
                rejected_row_count: rejectedRowCount,
                duplicate_row_count: duplicateRowCount,
                clean_file_count: cleanPartNumber,
                rejected_file_count: rejectedPartNumber,
                output_format: "jsonl",
                excluded_from_clean_output: [
                    "url",
                    "user_name",
                    "user_friendly_name",
                    "computer_name",
                    "data",
                ],
                status: "Succeeded",
                completed_at_utc: new Date().toISOString(),
            };

            await uploadJson(
                silverContainerClient,
                `${silverPrefix}/manifest.json`,
                silverManifest
            );

            const silverFinishedAt = new Date();
            await batchLogClient.upsertEntity(
                {
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    silver_path: silverPath,
                    silver_status: "Succeeded",
                    current_stage: "Gold",
                    clean_row_count: cleanRowCount,
                    rejected_row_count: rejectedRowCount,
                    duplicate_row_count: duplicateRowCount,
                    silver_file_count: cleanPartNumber,
                    silver_finished_at: silverFinishedAt,
                    updated_at: silverFinishedAt,
                    failed_stage: "",
                    error_category: "",
                    error_code: "",
                    error_message: "",
                },
                "Merge"
            );

            context.log(
                `Silver succeeded: batch_id=${batchId}, source=${sourceRowCount}, clean=${cleanRowCount}, rejected=${rejectedRowCount}, duplicates=${duplicateRowCount}`
            );

            return {
                status: 200,
                jsonBody: {
                    success: true,
                    batch_id: batchId,
                    bronze_path: `${bronzeContainer}/${bronzePrefix}`,
                    silver_path: silverPath,
                    source_row_count: sourceRowCount,
                    clean_row_count: cleanRowCount,
                    rejected_row_count: rejectedRowCount,
                    duplicate_row_count: duplicateRowCount,
                    clean_file_count: cleanPartNumber,
                    output_format: "jsonl",
                },
            };
        } catch (error) {
            context.error("Silver Function failed", {
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
                            current_stage: "Silver",
                            silver_status: "Failed",
                            failed_stage: "Silver",
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
                    context.error("Silver failed to update BatchLog failure state", {
                        message: logError.message,
                    });
                }
            }

            return {
                status: Number.isInteger(error.httpStatus) ? error.httpStatus : 500,
                jsonBody: {
                    success: false,
                    batch_id: batchId,
                    error_code: error.errorCode ?? "UNKNOWN",
                    error_category: error.errorCategory ?? "Transient",
                    message: error.message,
                },
            };
        }
    },
});
