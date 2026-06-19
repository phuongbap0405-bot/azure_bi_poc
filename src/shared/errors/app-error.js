"use strict";

class AppError extends Error {
    constructor(message, options = {}) {
        super(message);
        this.name = "AppError";
        this.errorCode = options.errorCode ?? "UNKNOWN";
        this.errorCategory = options.errorCategory ?? "Permanent";
        
        // Preserve Bronze's "isPermanent", default true unless category is Transient
        if (options.isPermanent !== undefined) {
            this.isPermanent = options.isPermanent;
        } else {
            this.isPermanent = this.errorCategory !== "Transient";
        }
        
        this.httpStatus = options.httpStatus;
        this.retryAfterMs = options.retryAfterMs;
    }
}

module.exports = { AppError };
