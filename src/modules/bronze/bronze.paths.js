"use strict";

const { sanitizeTableKey } = require("../../shared/utils/path");
const { compactUtcTimestamp } = require("../../shared/utils/date");
const { AppError } = require("../../shared/errors/app-error");

function generateBatchId(pipelineType, entityName, triggerTimeUtc, runId) {
    const compact = compactUtcTimestamp(triggerTimeUtc);
    const shortRunId = String(runId)
        .replace(/[^A-Za-z0-9]/g, "")
        .slice(0, 8)
        .toLowerCase();

    if (!shortRunId) {
        throw new AppError("adf_pipeline_run_id does not contain usable characters", {
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
        throw new AppError("Cannot determine BatchLog partition month", {
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

module.exports = {
    generateBatchId,
    extractBatchMonth,
    buildBatchLogPartitionKey,
    buildCheckpointPartitionKey,
};
