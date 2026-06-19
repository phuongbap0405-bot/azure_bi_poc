"use strict";

const errors = require("./controlio.errors");

function sleep(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function getRetryAfterMilliseconds(response) {
    const value = response.headers.get("retry-after");
    if (!value) {
        return null;
    }

    const seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) {
        return seconds * 1000;
    }

    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
        return Math.max(0, date.getTime() - Date.now());
    }

    return null;
}

async function readResponseTextSafely(response) {
    try {
        return await response.text();
    } catch {
        return "";
    }
}

class ControlioClient {
    async fetchControlioPageWithRetry({
        url,
        bearerToken,
        logger,
        maxAttempts,
        timeoutMs,
    }) {
        let lastError;

        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
            const abortController = new AbortController();
            const timeoutHandle = setTimeout(() => abortController.abort(), timeoutMs);

            try {
                const response = await fetch(url, {
                    method: "GET",
                    headers: {
                        Authorization: `Bearer ${bearerToken}`,
                        Accept: "application/json",
                    },
                    signal: abortController.signal,
                });

                if (!response.ok) {
                    const responseText = await readResponseTextSafely(response);
                    const retryAfterMs = response.status === 429
                        ? getRetryAfterMilliseconds(response)
                        : null;

                    throw errors.createControlioError(response, responseText, retryAfterMs);
                }

                let rawResponse;
                try {
                    rawResponse = await response.json();
                } catch (parseError) {
                    throw errors.createControlioInvalidJsonError(parseError.message);
                }

                if (!rawResponse || typeof rawResponse !== "object" || !Array.isArray(rawResponse.data)) {
                    throw errors.createControlioInvalidResponseError();
                }

                return {
                    rows: rawResponse.data,
                    rawResponse,
                    attemptCount: attempt,
                };
            } catch (error) {
                const isTimeout = error.name === "AbortError";
                let normalizedError = isTimeout
                    ? errors.createControlioTimeoutError(timeoutMs)
                    : error;

                if (
                    normalizedError.isPermanent === undefined &&
                    !(normalizedError.errorCode || normalizedError.errorCategory)
                ) {
                    normalizedError = errors.createControlioNetworkError();
                }

                if (normalizedError.isPermanent) {
                    throw normalizedError;
                }

                lastError = normalizedError;

                if (attempt < maxAttempts) {
                    const exponentialBackoffMs = 1000 * 2 ** (attempt - 1);
                    const retryDelayMs = Math.min(
                        normalizedError.retryAfterMs ?? exponentialBackoffMs,
                        60_000
                    );

                    if (logger) {
                        logger.warn(
                            `Controlio request attempt ${attempt}/${maxAttempts} failed ` +
                            `(${normalizedError.errorCode ?? "UNKNOWN"}). Retrying in ${retryDelayMs} ms.`
                        );
                    }

                    await sleep(retryDelayMs);
                }
            } finally {
                clearTimeout(timeoutHandle);
            }
        }

        throw errors.createControlioExhaustedError(lastError);
    }
}

module.exports = { ControlioClient };
