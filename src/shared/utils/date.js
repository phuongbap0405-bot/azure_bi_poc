"use strict";

const { AppError } = require("../errors/app-error");

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

function parseOptionalUtcDate(value, fieldName) {
    if (value === undefined || value === null || value === "") {
        return undefined;
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        throw new AppError(`${fieldName} must be a valid UTC date-time`, {
            errorCode: "INVALID_DATETIME",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return date;
}

function validateDateRange(startTime, endTime) {
    if (!startTime || !endTime) {
        throw new AppError("start_time and end_time are required", {
            errorCode: "MISSING_DATE_RANGE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    if (!DATE_PATTERN.test(startTime) || !DATE_PATTERN.test(endTime)) {
        throw new AppError("start_time and end_time must use YYYY-MM-DD format", {
            errorCode: "INVALID_DATE_FORMAT",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const start = new Date(`${startTime}T00:00:00.000Z`);
    const end = new Date(`${endTime}T00:00:00.000Z`);

    if (
        Number.isNaN(start.getTime()) ||
        Number.isNaN(end.getTime()) ||
        start.toISOString().slice(0, 10) !== startTime ||
        end.toISOString().slice(0, 10) !== endTime
    ) {
        throw new AppError("start_time or end_time is not a valid calendar date", {
            errorCode: "INVALID_DATE_VALUE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    if (start > end) {
        throw new AppError("start_time cannot be later than end_time", {
            errorCode: "INVALID_DATE_RANGE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }
}

function compactUtcTimestamp(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        throw new AppError("trigger_time_utc must be a valid UTC date-time", {
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

function normalizeControlioUtcTimestamp(value, fieldName) {
    if (typeof value !== "string" || !value.trim()) {
        throw new AppError(`${fieldName} is required`, {
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
        throw new AppError(`${fieldName} is not a valid timestamp: ${value}`, {
            errorCode: "INVALID_TIMESTAMP",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return date.toISOString();
}

module.exports = {
    parseOptionalUtcDate,
    validateDateRange,
    compactUtcTimestamp,
    normalizeControlioUtcTimestamp,
};
