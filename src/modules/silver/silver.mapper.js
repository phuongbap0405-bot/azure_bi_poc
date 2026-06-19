"use strict";

const { AppError } = require("../../shared/errors/app-error");
const { toInteger, toBoolean, nullableString } = require("../../shared/utils/validation");
const { normalizeControlioUtcTimestamp } = require("../../shared/utils/date");
const { createUniqueKey } = require("../../shared/utils/hashing");

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

function normalizeTimelineRecord(rawRecord, metadata) {
    if (!rawRecord || typeof rawRecord !== "object" || Array.isArray(rawRecord)) {
        throw new AppError("Timeline record must be an object", {
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

module.exports = {
    extractUrlDomain,
    normalizeTimelineRecord,
    createRejectedRecord,
};
