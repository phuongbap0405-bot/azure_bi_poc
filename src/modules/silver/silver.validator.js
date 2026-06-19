"use strict";

const { AppError } = require("../../shared/errors/app-error");
const { sanitizeTableKey } = require("../../shared/utils/path");
const { normalizeBronzePrefix } = require("../../shared/utils/path");

function validateRequest(body, bronzeContainer) {
    if (!body || typeof body !== "object") {
        throw new AppError("Request body must be valid JSON", {
            errorCode: "INVALID_JSON_BODY",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const batchId = body.batch_id ? sanitizeTableKey(body.batch_id) : null;
    if (!batchId) {
        throw new AppError("batch_id is required", {
            errorCode: "MISSING_BATCH_ID",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const bronzePrefix = normalizeBronzePrefix(body.bronze_path, bronzeContainer);

    return {
        batchId,
        bronzePrefix,
    };
}

module.exports = { validateRequest };
