"use strict";

const { AppError } = require("../../shared/errors/app-error");
const { sanitizeTableKey, sanitizeBlobPathSegment } = require("../../shared/utils/path");

function extractBatchMonth(batchId, fallbackDate) {
    const match = String(batchId).match(/_(\d{6})\d{2}T\d{6}Z_/);
    if (match) {
        return match[1];
    }

    const date = new Date(fallbackDate);
    if (Number.isNaN(date.getTime())) {
        throw new AppError("Cannot determine BatchLog partition month", {
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

function buildSilverPrefix(sourceName, entityName, startTime, endTime, batchId) {
    return [
        sanitizeBlobPathSegment(sourceName),
        sanitizeBlobPathSegment(entityName),
        `start_date=${sanitizeBlobPathSegment(startTime)}`,
        `end_date=${sanitizeBlobPathSegment(endTime)}`,
        `batch_id=${sanitizeBlobPathSegment(batchId)}`,
    ].join("/");
}

module.exports = {
    extractBatchMonth,
    buildBatchLogPartitionKey,
    buildSilverPrefix,
};
