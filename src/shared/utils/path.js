"use strict";

const { AppError } = require("../errors/app-error");

function sanitizeTableKey(value) {
    return String(value)
        .replace(/[\/\\#?]/g, "-")
        .replace(/[\u0000-\u001F\u007F]/g, "-");
}

function sanitizeBlobPathSegment(value) {
    return encodeURIComponent(String(value));
}

function normalizeBronzePrefix(bronzePath, bronzeContainer) {
    let value = String(bronzePath ?? "").trim().replace(/^\/+|\/+$/g, "");
    if (!value) {
        throw new AppError("bronze_path is required", {
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
        throw new AppError("bronze_path is invalid", {
            errorCode: "INVALID_BRONZE_PATH",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    return value;
}

module.exports = {
    sanitizeTableKey,
    sanitizeBlobPathSegment,
    normalizeBronzePrefix,
};
