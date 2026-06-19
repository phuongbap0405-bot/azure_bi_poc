"use strict";

const { AppError } = require("../../shared/errors/app-error");

const TRANSIENT_HTTP_STATUS_CODES = new Set([429, 500, 502, 503, 504]);

function createControlioError(response, responseText, retryAfterMs = null) {
    const isTransient = TRANSIENT_HTTP_STATUS_CODES.has(response.status);
    const error = new AppError(
        `Controlio API returned HTTP ${response.status}: ${responseText.slice(0, 300)}`,
        {
            errorCode: `CONTROLIO_${response.status}`,
            errorCategory:
                response.status === 401 || response.status === 403
                    ? "Configuration"
                    : isTransient
                        ? "Transient"
                        : "Permanent",
            isPermanent: !isTransient,
            httpStatus: isTransient ? 503 : 500,
            retryAfterMs,
        }
    );
    return error;
}

function createControlioInvalidJsonError(message) {
    return new AppError(`Controlio returned invalid JSON: ${message}`, {
        errorCode: "CONTROLIO_INVALID_JSON",
        errorCategory: "Permanent",
        isPermanent: true,
        httpStatus: 500,
    });
}

function createControlioInvalidResponseError() {
    return new AppError("Controlio response must contain a data array", {
        errorCode: "CONTROLIO_INVALID_RESPONSE",
        errorCategory: "Permanent",
        isPermanent: true,
        httpStatus: 500,
    });
}

function createControlioTimeoutError(timeoutMs) {
    return new AppError(`Controlio request timed out after ${timeoutMs} ms`, {
        errorCode: "CONTROLIO_TIMEOUT",
        errorCategory: "Transient",
        isPermanent: false,
        httpStatus: 503,
    });
}

function createControlioNetworkError() {
    return new AppError("Controlio network error", {
        errorCode: "CONTROLIO_NETWORK_ERROR",
        errorCategory: "Transient",
        isPermanent: false,
        httpStatus: 503,
    });
}

function createControlioExhaustedError(lastError = null) {
    if (lastError) {
        lastError.errorCode = lastError.errorCode ?? "CONTROLIO_RETRY_EXHAUSTED";
        lastError.errorCategory = lastError.errorCategory ?? "Transient";
        lastError.isPermanent = false;
        lastError.httpStatus = 503;
        return lastError;
    }

    return new AppError("Controlio retry attempts were exhausted", {
        errorCode: "CONTROLIO_RETRY_EXHAUSTED",
        errorCategory: "Transient",
        isPermanent: false,
        httpStatus: 503,
    });
}

module.exports = {
    createControlioError,
    createControlioInvalidJsonError,
    createControlioInvalidResponseError,
    createControlioTimeoutError,
    createControlioNetworkError,
    createControlioExhaustedError,
};
