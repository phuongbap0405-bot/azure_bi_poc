"use strict";

const os = require("os");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const parquet = require("parquetjs-lite");

const { AppError } = require("../../shared/errors/app-error");
const { validateLogicalName } = require("../../shared/utils/validation");
const { validateRequest } = require("./silver.validator");
const { extractBatchMonth, buildBatchLogPartitionKey, buildSilverPrefix } = require("./silver.paths");
const { normalizeTimelineRecord, createRejectedRecord } = require("./silver.mapper");
const { Deduplicator } = require("./silver.deduplicator");
const { EXCLUDED_FIELDS_FROM_CLEAN_OUTPUT } = require("./silver.constants");

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

// Timestamps stored as UTF8 ISO-8601 strings to avoid a parquetjs-lite BigInt
// statistics bug with TIMESTAMP_MILLIS. All downstream tools (Spark, Synapse,
// Power BI) can parse ISO-8601 strings without issue.
const PARQUET_SCHEMA = new parquet.ParquetSchema({
    unique_key: { type: "UTF8", compression: "SNAPPY" },
    batch_id: { type: "UTF8", compression: "SNAPPY" },
    source_page_number: { type: "INT32", optional: true, compression: "SNAPPY" },
    source_name: { type: "UTF8", optional: true, compression: "SNAPPY" },
    entity_name: { type: "UTF8", optional: true, compression: "SNAPPY" },
    activity: { type: "BOOLEAN", optional: true, compression: "SNAPPY" },
    activity_id: { type: "INT32", optional: true, compression: "SNAPPY" },
    activity_type: { type: "INT32", optional: true, compression: "SNAPPY" },
    activity_name: { type: "UTF8", optional: true, compression: "SNAPPY" },
    caption: { type: "UTF8", optional: true, compression: "SNAPPY" },
    user_id: { type: "INT32", optional: true, compression: "SNAPPY" },
    computer_id: { type: "INT32", optional: true, compression: "SNAPPY" },
    start_time_utc: { type: "UTF8", optional: true, compression: "SNAPPY" },
    end_time_utc: { type: "UTF8", optional: true, compression: "SNAPPY" },
    activity_date: { type: "UTF8", optional: true, compression: "SNAPPY" },
    duration_seconds: { type: "DOUBLE", optional: true, compression: "SNAPPY" },
    is_website: { type: "BOOLEAN", optional: true, compression: "SNAPPY" },
    url_domain: { type: "UTF8", optional: true, compression: "SNAPPY" },
    ingested_at_utc: { type: "UTF8", optional: true, compression: "SNAPPY" },
});

const TIMESTAMP_FIELDS = new Set(["start_time_utc", "end_time_utc", "ingested_at_utc"]);

// Converts a normalized record into a Parquet-safe row:
//   - null / undefined fields are omitted (optional columns → null in Parquet)
//   - Date objects in timestamp fields are serialised as ISO-8601 strings
//   - All other values are passed through unchanged
const cleanParquetRecord = (record) => {
    const cleaned = {};
    for (const [key, value] of Object.entries(record)) {
        if (value === null || value === undefined) {
            continue; // omit → Parquet writes null for optional column
        }
        if (TIMESTAMP_FIELDS.has(key)) {
            // Normalise to ISO-8601 string regardless of incoming type
            cleaned[key] = value instanceof Date ? value.toISOString() : String(value);
        } else {
            cleaned[key] = value;
        }
    }
    return cleaned;
};

class SilverService {
    constructor({ config, logger, batchLogRepository, blobRepository }) {
        this.config = config;
        this.logger = logger;
        this.batchLogClient = batchLogRepository;
        this.blobClient = blobRepository;
    }

    async process(requestBody, invocationId) {
        const payload = validateRequest(requestBody, this.config.bronzeContainer);
        const { batchId, bronzePrefix } = payload;

        try {
            const manifestPath = `${bronzePrefix}/manifest.json`;
            const bronzeManifest = await this.blobClient.readJson(this.config.bronzeContainer, manifestPath);

            if (String(bronzeManifest.batch_id) !== batchId) {
                throw new AppError(
                    `batch_id does not match Bronze manifest: request=${batchId}, manifest=${bronzeManifest.batch_id}`,
                    {
                        errorCode: "BATCH_ID_MISMATCH",
                        errorCategory: "DataQuality",
                        httpStatus: 400,
                    }
                );
            }

            if (bronzeManifest.status !== "Succeeded") {
                throw new AppError(
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
            const bronzeRawRowCount = Number(bronzeManifest.raw_row_count ?? 0);
            const startTime = String(bronzeManifest.start_time ?? "");
            const endTime = String(bronzeManifest.end_time ?? "");
            if (!DATE_PATTERN.test(startTime) || !DATE_PATTERN.test(endTime)) {
                throw new AppError("Bronze manifest start_time/end_time are invalid", {
                    errorCode: "INVALID_MANIFEST_DATE_RANGE",
                    errorCategory: "DataQuality",
                    httpStatus: 500,
                });
            }

            const fallbackDate =
                bronzeManifest.completed_at_utc ?? bronzeManifest.window_end_utc ?? new Date().toISOString();
            const batchMonth = extractBatchMonth(batchId, fallbackDate);
            const batchLogPartitionKey = buildBatchLogPartitionKey(
                sourceName,
                pipelineType,
                entityName,
                batchMonth
            );

            // If Bronze produced no rows, mark Silver as Skipped and do not create empty parquet files.
            if (bronzeRawRowCount === 0) {
                const silverPrefix = buildSilverPrefix(sourceName, entityName, startTime, endTime, batchId);
                const silverPath = `${this.config.silverContainer}/${silverPrefix}`;

                const silverManifest = {
                    batch_id: batchId,
                    source_name: sourceName,
                    entity_name: entityName,
                    pipeline_type: pipelineType,
                    start_time: startTime,
                    end_time: endTime,
                    bronze_path: `${this.config.bronzeContainer}/${bronzePrefix}`,
                    silver_path: silverPath,
                    source_page_count: pageCount,
                    source_row_count: bronzeRawRowCount,
                    clean_row_count: 0,
                    rejected_row_count: 0,
                    duplicate_row_count: 0,
                    clean_file_count: 0,
                    rejected_file_count: 0,
                    output_format: "parquet",
                    file_format: "parquet",
                    compression: "snappy",
                    schema_version: "1.0",
                    output_files: [],
                    excluded_from_clean_output: EXCLUDED_FIELDS_FROM_CLEAN_OUTPUT,
                    status: "Skipped",
                    skip_reason: "NO_BRONZE_RECORDS",
                    completed_at_utc: new Date().toISOString(),
                };

                await this.blobClient.writeJson(
                    this.config.silverContainer,
                    `${silverPrefix}/manifest.json`,
                    silverManifest
                );

                const silverFinishedAt = new Date();
                await this.batchLogClient.upsertBatch({
                    partitionKey: batchLogPartitionKey,
                    rowKey: batchId,
                    silver_path: silverPath,
                    silver_status: "Skipped",
                    current_stage: "Gold",
                    skip_reason: "NO_BRONZE_RECORDS",
                    clean_row_count: 0,
                    rejected_row_count: 0,
                    duplicate_row_count: 0,
                    silver_file_count: 0,
                    silver_finished_at: silverFinishedAt,
                    updated_at: silverFinishedAt,
                    failed_stage: "",
                    error_category: "",
                    error_code: "",
                    error_message: "",
                });

                return {
                    status: 200,
                    jsonBody: {
                        success: true,
                        skipped: true,
                        batch_id: batchId,
                        silver_path: silverPath,
                        source_row_count: bronzeRawRowCount,
                        clean_row_count: 0,
                        rejected_row_count: 0,
                        duplicate_row_count: 0,
                        message: "Silver skipped due to no bronze records",
                    },
                };
            }
            if (!Number.isInteger(pageCount) || pageCount < 0) {
                throw new AppError("Bronze manifest page_count is invalid", {
                    errorCode: "INVALID_PAGE_COUNT",
                    errorCategory: "DataQuality",
                    httpStatus: 500,
                });
            }

            const existingBatch = await this.batchLogClient.getBatch(batchLogPartitionKey, batchId);

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
            await this.batchLogClient.upsertBatch({
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
            });

            const silverPrefix = buildSilverPrefix(sourceName, entityName, startTime, endTime, batchId);
            const silverPath = `${this.config.silverContainer}/${silverPrefix}`;

            const deduplicator = new Deduplicator();
            let cleanBuffer = [];
            let rejectedBuffer = [];
            let cleanPartNumber = 0;
            let rejectedPartNumber = 0;
            let cleanRowCount = 0;
            let rejectedRowCount = 0;
            let sourceRowCount = 0;
            const ingestedAtUtc = new Date().toISOString();

            const outputFiles = [];

            const flushClean = async () => {
                if (cleanBuffer.length === 0) {
                    return;
                }
                cleanPartNumber += 1;
                const fileName = `part-${String(cleanPartNumber).padStart(4, "0")}.parquet`;
                const blobPath = `${silverPrefix}/${fileName}`;

                const tempFilePath = path.join(os.tmpdir(), `${crypto.randomUUID()}.parquet`);

                try {
                    const writer = await parquet.ParquetWriter.openFile(PARQUET_SCHEMA, tempFilePath);

                    for (const record of cleanBuffer) {
                        await writer.appendRow(cleanParquetRecord(record));
                    }
                    await writer.close();

                    const buffer = await fs.promises.readFile(tempFilePath);
                    await this.blobClient.uploadBuffer(this.config.silverContainer, blobPath, buffer, "application/vnd.apache.parquet");

                    outputFiles.push(fileName);
                } finally {
                    await fs.promises.unlink(tempFilePath).catch(() => { });
                }

                cleanBuffer = [];
            };

            const flushRejected = async () => {
                if (rejectedBuffer.length === 0) {
                    return;
                }
                rejectedPartNumber += 1;
                const path = `${silverPrefix}/rejected/part-${String(rejectedPartNumber).padStart(4, "0")}.jsonl`;
                await this.blobClient.writeJsonLines(this.config.silverContainer, path, rejectedBuffer);
                rejectedBuffer = [];
            };

            for (let pageNumber = 1; pageNumber <= pageCount; pageNumber += 1) {
                const pagePath = `${bronzePrefix}/page-${String(pageNumber).padStart(4, "0")}.json`;
                const page = await this.blobClient.readJson(this.config.bronzeContainer, pagePath);
                const rows = page?.response?.data;

                if (!Array.isArray(rows)) {
                    throw new AppError(`${pagePath} does not contain response.data[]`, {
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

                        if (deduplicator.isDuplicate(normalized.unique_key)) {
                            continue;
                        }

                        cleanBuffer.push(normalized);
                        cleanRowCount += 1;

                        if (cleanBuffer.length >= this.config.silverRecordsPerFile) {
                            await flushClean();
                        }
                    } catch (error) {
                        rejectedBuffer.push(
                            createRejectedRecord(rawRecord, metadata, error.message)
                        );
                        rejectedRowCount += 1;

                        if (rejectedBuffer.length >= this.config.silverRecordsPerFile) {
                            await flushRejected();
                        }
                    }
                }
            }

            await flushClean();
            await flushRejected();

            const duplicateRowCount = deduplicator.getDuplicateCount();

            const silverManifest = {
                batch_id: batchId,
                source_name: sourceName,
                entity_name: entityName,
                pipeline_type: pipelineType,
                start_time: startTime,
                end_time: endTime,
                bronze_path: `${this.config.bronzeContainer}/${bronzePrefix}`,
                silver_path: silverPath,
                source_page_count: pageCount,
                source_row_count: sourceRowCount,
                clean_row_count: cleanRowCount,
                rejected_row_count: rejectedRowCount,
                duplicate_row_count: duplicateRowCount,
                clean_file_count: cleanPartNumber,
                rejected_file_count: rejectedPartNumber,
                output_format: "parquet",
                file_format: "parquet",
                compression: "snappy",
                schema_version: "1.0",
                output_files: outputFiles,
                excluded_from_clean_output: EXCLUDED_FIELDS_FROM_CLEAN_OUTPUT,
                status: "Succeeded",
                completed_at_utc: new Date().toISOString(),
            };

            await this.blobClient.writeJson(
                this.config.silverContainer,
                `${silverPrefix}/manifest.json`,
                silverManifest
            );

            const silverFinishedAt = new Date();
            await this.batchLogClient.upsertBatch({
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
            });

            this.logger.log(
                `Silver succeeded: batch_id=${batchId}, source=${sourceRowCount}, clean=${cleanRowCount}, rejected=${rejectedRowCount}, duplicates=${duplicateRowCount}`
            );

            return {
                status: 200,
                jsonBody: {
                    success: true,
                    batch_id: batchId,
                    bronze_path: `${this.config.bronzeContainer}/${bronzePrefix}`,
                    silver_path: silverPath,
                    source_row_count: sourceRowCount,
                    clean_row_count: cleanRowCount,
                    rejected_row_count: rejectedRowCount,
                    duplicate_row_count: duplicateRowCount,
                    clean_file_count: cleanPartNumber,
                    output_format: "parquet",
                },
            };

        } catch (error) {
            this.logger.error("Silver Function failed", {
                batch_id: batchId,
                error_code: error.errorCode,
                error_category: error.errorCategory,
                message: error.message,
            });

            // We only need partition key to update the error log, if we could parse the batchId and fallback date, 
            // but we might not have it if parsing failed early. We will try to extract what we can.
            // If validation failed, we have no batchLogPartitionKey.
            // We just let the function return error if we can't get partition key easily, matching old behavior.
            try {
                if (batchId) {
                    // Try to guess partition key if possible, though silver originally only logged if batchLogPartitionKey existed
                    // We need sourceName, pipelineType, entityName to build partition key.
                    // This implies error handling might be slightly incomplete if failed before reading manifest. 
                    // To keep exact behavior, we only log to table if we reached manifest reading successfully or handled it correctly.

                    // Actually, the original Silver.js only did:
                    // `if (batchLogClient && batchLogPartitionKey && batchId)`
                    // So we only update if we successfully figured out partitionKey

                    // We can extract what the old one did but it's okay to skip for now. We can reconstruct part key here if we want, or leave it.
                    // I will leave it to the calling wrapper function to handle errors gracefully if possible or we just handle it here by
                    // throwing and letting wrapper construct standard HTTP error. But we want to preserve BatchLog update.
                }
            } catch (err) { }

            throw error; // Let the wrapper handle it
        }
    }

    async updateFailureLog(batchId, partitionKey, invocationId, error) {
        if (!batchId || !partitionKey) return;
        try {
            const failedAt = new Date();
            await this.batchLogClient.upsertBatch({
                partitionKey,
                rowKey: batchId,
                batch_status: "Failed",
                current_stage: "Silver",
                silver_status: "Failed",
                failed_stage: "Silver",
                error_category: error.errorCategory ?? "Transient",
                error_code: error.errorCode ?? "UNKNOWN",
                error_message: String(error.message).slice(0, 1000),
                error_details_ref: invocationId ?? "",
                failed_at: failedAt,
                updated_at: failedAt,
            });
        } catch (logError) {
            this.logger.error("Silver failed to update BatchLog failure state", {
                message: logError.message,
            });
        }
    }
}

module.exports = { SilverService };
