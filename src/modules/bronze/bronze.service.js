"use strict";

const { AppError } = require("../../shared/errors/app-error");
const { sanitizeTableKey, sanitizeBlobPathSegment } = require("../../shared/utils/path");
const { buildCursorFromLastRow } = require("../../infrastructure/controlio/controlio.pagination");
const {
    generateBatchId,
    extractBatchMonth,
    buildBatchLogPartitionKey,
    buildCheckpointPartitionKey,
} = require("./bronze.paths");
const { parseRequestPayload } = require("./bronze.mapper");

class BronzeService {
    constructor({
        config,
        logger,
        batchLogRepository,
        checkpointRepository,
        blobRepository,
        keyVaultRepository,
        controlioClient,
    }) {
        this.config = config;
        this.logger = logger;
        this.batchLogClient = batchLogRepository;
        this.checkpointClient = checkpointRepository;
        this.blobClient = blobRepository;
        this.keyVaultClient = keyVaultRepository;
        this.controlioClient = controlioClient;
    }

    async process(requestBody, invocationId) {
        const payload = parseRequestPayload(requestBody);

        const batchId = payload.resumeBatchId
            ? sanitizeTableKey(payload.resumeBatchId)
            : generateBatchId(
                  payload.pipelineType,
                  payload.entityName,
                  payload.triggerTimeUtcText,
                  payload.runId
              );

        const batchMonth = extractBatchMonth(batchId, payload.triggerTimeUtcText);
        const batchLogPartitionKey = buildBatchLogPartitionKey(
            payload.sourceName,
            payload.pipelineType,
            payload.entityName,
            batchMonth
        );

        this.logger.log(`Bronze started: batch_id=${batchId}, partition_key=${batchLogPartitionKey}`);

        try {
            const existingBatch = await this.batchLogClient.getBatch(batchLogPartitionKey, batchId);

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
                existingBatch && existingBatch.current_adf_pipeline_run_id !== payload.runId;
            const runMode =
                payload.runMode ??
                (payload.resumeBatchId || isNewPipelineRun
                    ? "Resume"
                    : payload.triggerType === "Manual"
                        ? "Manual"
                        : "Scheduled");

            if (!existingBatch) {
                const newBatchEntity = {
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    batch_id: batchId,
                    initial_adf_pipeline_run_id: String(payload.runId),
                    current_adf_pipeline_run_id: String(payload.runId),
                    adf_pipeline_name: String(payload.adfPipelineName),
                    trigger_type: String(payload.triggerType),
                    trigger_time_utc: payload.triggerTimeUtc,
                    run_mode: runMode,
                    rerun_count: 0,
                    pipeline_type: payload.pipelineType,
                    source_name: payload.sourceName,
                    entity_name: payload.entityName,
                    start_time: payload.startTime,
                    end_time: payload.endTime,
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
                    schema_version: this.config.schemaVersion,
                    code_version: this.config.codeVersion,
                    created_at: now,
                    updated_at: now,
                };

                if (payload.windowStartUtc) {
                    newBatchEntity.window_start_utc = payload.windowStartUtc;
                }
                if (payload.windowEndUtc) {
                    newBatchEntity.window_end_utc = payload.windowEndUtc;
                }

                await this.batchLogClient.upsertBatch(newBatchEntity);
            } else {
                await this.batchLogClient.upsertBatch({
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    current_adf_pipeline_run_id: String(payload.runId),
                    run_mode: runMode,
                    rerun_count:
                        Number(existingBatch.rerun_count ?? 0) +
                        (isNewPipelineRun ? 1 : 0),
                    updated_at: now,
                });
            }

            const checkpointPartitionKey = buildCheckpointPartitionKey(
                payload.sourceName,
                payload.pipelineType
            );
            const checkpointEntity = await this.checkpointClient.getCheckpoint(
                checkpointPartitionKey,
                payload.entityName
            );

            const checkpointCursor = checkpointEntity?.committed_cursor_prev || null;
            const originalInputCursor =
                existingBatch?.input_cursor_prev ||
                (payload.pipelineType === "raw" ? checkpointCursor : null);

            const resumeCursor =
                existingBatch?.last_persisted_cursor_prev || originalInputCursor;

            const startedAt = existingBatch?.started_at ?? now;
            const bronzeStartedAt = existingBatch?.bronze_started_at ?? now;

            await this.batchLogClient.upsertBatch({
                partitionKey: batchLogPartitionKey,
                rowKey: batchId,
                input_cursor_prev: originalInputCursor ?? "",
                batch_status: "Running",
                current_stage: "Bronze",
                bronze_status: "Running",
                current_adf_pipeline_run_id: String(payload.runId),
                started_at: startedAt,
                bronze_started_at: bronzeStartedAt,
                updated_at: new Date(),
                failed_stage: "",
                error_category: "",
                error_code: "",
                error_message: "",
            });

            const tokenValue = await this.keyVaultClient.getSecretValue(this.config.controlioTokenSecretName);
            if (!tokenValue) {
                throw new AppError("Controlio API token in Key Vault is empty", {
                    errorCode: "EMPTY_API_TOKEN",
                    errorCategory: "Configuration",
                    httpStatus: 500,
                });
            }

            const blobPrefix = [
                sanitizeBlobPathSegment(payload.sourceName),
                sanitizeBlobPathSegment(payload.entityName),
                `start_date=${sanitizeBlobPathSegment(payload.startTime)}`,
                `end_date=${sanitizeBlobPathSegment(payload.endTime)}`,
                `batch_id=${sanitizeBlobPathSegment(batchId)}`,
            ].join("/");
            const bronzePath = `${this.config.bronzeContainer}/${blobPrefix}`;

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
                if (pageNumber >= this.config.maxPagesPerBatch) {
                    throw new AppError(
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
                    this.config.controlioBaseUrl.endsWith("/")
                        ? this.config.controlioBaseUrl
                        : `${this.config.controlioBaseUrl}/`
                );

                apiUrl.searchParams.set("start_time", payload.startTime);
                apiUrl.searchParams.set("end_time", payload.endTime);
                apiUrl.searchParams.set("limit", String(this.config.controlioPageLimit));
                apiUrl.searchParams.set("sort_direction", "asc");

                if (currentPrev) {
                    apiUrl.searchParams.set("prev", currentPrev);
                }
                if (payload.activities) {
                    apiUrl.searchParams.set("activities", payload.activities);
                }
                if (payload.users) {
                    apiUrl.searchParams.set("users", payload.users);
                }
                if (payload.departments) {
                    apiUrl.searchParams.set("departments", payload.departments);
                }
                if (payload.stringToMatch) {
                    apiUrl.searchParams.set("string_to_match", payload.stringToMatch);
                }
                if (payload.activityType !== null) {
                    apiUrl.searchParams.set("activity_type", String(payload.activityType));
                }

                this.logger.log(`Bronze fetching page ${nextPageNumber} for batch_id=${batchId}`);

                const { rows, rawResponse, attemptCount } =
                    await this.controlioClient.fetchControlioPageWithRetry({
                        url: apiUrl.toString(),
                        bearerToken: tokenValue,
                        logger: this.logger,
                        maxAttempts: this.config.controlioMaxApiAttempts,
                        timeoutMs: this.config.controlioRequestTimeoutMs,
                    });

                totalApiCalls += attemptCount;

                const pageOutputCursor = buildCursorFromLastRow(rows);
                const nextPrev = rows.length === this.config.controlioPageLimit ? pageOutputCursor : null;

                if (nextPrev && seenCursors.has(nextPrev)) {
                    throw new AppError(`Repeated pagination cursor detected: ${nextPrev}`, {
                        errorCode: "REPEATED_CURSOR",
                        errorCategory: "Permanent",
                        httpStatus: 500,
                    });
                }

                const requestParameters = {
                    start_time: payload.startTime,
                    end_time: payload.endTime,
                    limit: this.config.controlioPageLimit,
                    sort_direction: "asc",
                    prev: currentPrev ?? null,
                    activities: payload.activities,
                    users: payload.users,
                    departments: payload.departments,
                    string_to_match: payload.stringToMatch,
                    activity_type: payload.activityType,
                };

                const pagePath = `${blobPrefix}/page-${String(nextPageNumber).padStart(4, "0")}.json`;
                const pageContent = {
                    batch_id: batchId,
                    page_number: nextPageNumber,
                    input_prev: currentPrev ?? null,
                    output_prev: pageOutputCursor,
                    next_request_prev: nextPrev,
                    row_count: rows.length,
                    fetched_at_utc: new Date().toISOString(),
                    request_parameters: requestParameters,
                    response: rawResponse,
                };

                await this.blobClient.writeJson(this.config.bronzeContainer, pagePath, pageContent);

                pageNumber = nextPageNumber;
                totalRows += rows.length;
                lastPersistedCursor = pageOutputCursor ?? currentPrev;
                paginationComplete = nextPrev === null;

                await this.batchLogClient.upsertBatch({
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    page_count: pageNumber,
                    raw_row_count: totalRows,
                    api_call_count: totalApiCalls,
                    last_persisted_cursor_prev: lastPersistedCursor ?? "",
                    bronze_pagination_complete: paginationComplete,
                    updated_at: new Date(),
                });

                if (nextPrev) {
                    seenCursors.add(nextPrev);
                    currentPrev = nextPrev;
                }
            }

            const outputCursorPrev = lastPersistedCursor;
            const manifestPath = `${blobPrefix}/manifest.json`;
            const manifestContent = {
                batch_id: batchId,
                source_name: payload.sourceName,
                entity_name: payload.entityName,
                pipeline_type: payload.pipelineType,
                adf_pipeline_run_id: String(payload.runId),
                start_time: payload.startTime,
                end_time: payload.endTime,
                window_start_utc: payload.windowStartUtcText,
                window_end_utc: payload.windowEndUtcText,
                input_cursor_prev: originalInputCursor,
                output_cursor_prev: outputCursorPrev,
                page_count: pageNumber,
                raw_row_count: totalRows,
                api_call_count: totalApiCalls,
                bronze_path: bronzePath,
                status: "Succeeded",
                completed_at_utc: new Date().toISOString(),
            };

            await this.blobClient.writeJson(this.config.bronzeContainer, manifestPath, manifestContent);

            const bronzeFinishedAt = new Date();
            await this.batchLogClient.upsertBatch({
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
            });

            this.logger.log(`Bronze succeeded: batch_id=${batchId}, pages=${pageNumber}, rows=${totalRows}`);

            return {
                status: 200,
                jsonBody: {
                    success: true,
                    batch_id: batchId,
                    source_name: payload.sourceName,
                    entity_name: payload.entityName,
                    pipeline_type: payload.pipelineType,
                    start_time: payload.startTime,
                    end_time: payload.endTime,
                    input_cursor_prev: originalInputCursor,
                    output_cursor_prev: outputCursorPrev,
                    bronze_path: bronzePath,
                    page_count: pageNumber,
                    raw_row_count: totalRows,
                    api_call_count: totalApiCalls,
                },
            };

        } catch (error) {
            this.logger.error("Bronze Function failed", {
                batch_id: batchId,
                error_code: error.errorCode,
                error_category: error.errorCategory,
                message: error.message,
            });

            if (batchId && batchLogPartitionKey) {
                try {
                    const failedAt = new Date();
                    await this.batchLogClient.upsertBatch({
                        partitionKey: batchLogPartitionKey,
                        rowKey: batchId,
                        batch_status: "Failed",
                        current_stage: "Bronze",
                        bronze_status: "Failed",
                        failed_stage: "Bronze",
                        error_category: error.errorCategory ?? "Transient",
                        error_code: error.errorCode ?? "UNKNOWN",
                        error_message: String(error.message).slice(0, 1000),
                        error_details_ref: invocationId ?? "",
                        failed_at: failedAt,
                        updated_at: failedAt,
                    });
                } catch (logError) {
                    this.logger.error("Bronze failed to update BatchLog failure state", {
                        message: logError.message,
                    });
                }
            }

            throw error;
        }
    }
}

module.exports = { BronzeService };
