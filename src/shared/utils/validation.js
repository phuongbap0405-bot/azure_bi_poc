"use strict";

const { AppError } = require("../errors/app-error");

const SAFE_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;

function validateLogicalName(value, fieldName) {
    if (!value || !SAFE_NAME_PATTERN.test(value)) {
        throw new AppError(
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
        throw new AppError(`${fieldName} must be an integer between 1 and ${maximum}`, {
            errorCode: "INVALID_CONFIGURATION",
            errorCategory: "Configuration",
            httpStatus: 500,
        });
    }

    return parsed;
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
        throw new AppError(`${fieldName} must be a comma-separated list of integer IDs`, {
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
        throw new AppError("activity_type must be 0, 1, or 2", {
            errorCode: "INVALID_ACTIVITY_TYPE",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return parsed;
}

function toInteger(value, fieldName) {
    const parsed = Number(value);
    if (!Number.isInteger(parsed)) {
        throw new AppError(`${fieldName} must be an integer`, {
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

module.exports = {
    validateLogicalName,
    parsePositiveInteger,
    normalizeCommaSeparatedIds,
    normalizeActivityType,
    toInteger,
    toBoolean,
    nullableString,
};
