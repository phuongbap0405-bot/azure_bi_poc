"use strict";

function getHttpStatusForError(error) {
    if (Number.isInteger(error.httpStatus)) {
        return error.httpStatus;
    }
    return error.errorCategory === "Transient" ? 503 : 500;
}

function buildErrorResponse(error, batchId = null) {
    return {
        status: getHttpStatusForError(error),
        jsonBody: {
            success: false,
            batch_id: batchId,
            error_code: error.errorCode ?? "UNKNOWN",
            error_category: error.errorCategory ?? "Transient",
            message: error.message,
        },
    };
}

module.exports = {
    getHttpStatusForError,
    buildErrorResponse,
};
