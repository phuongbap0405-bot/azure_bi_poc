"use strict";

class Logger {
    constructor(azureContext) {
        this.context = azureContext;
        this.baseContext = {};
    }

    setContext(key, value) {
        if (value !== undefined) {
            this.baseContext[key] = value;
        }
    }

    log(message, additionalContext = {}) {
        const payload = this._formatMessage(message, additionalContext);
        this.context.log(payload);
    }

    warn(message, additionalContext = {}) {
        const payload = this._formatMessage(message, additionalContext);
        this.context.warn(payload);
    }

    error(message, errorOrContext = {}) {
        const payload = this._formatMessage(message, errorOrContext);
        this.context.error(payload);
    }

    _formatMessage(message, additionalContext) {
        const combined = { ...this.baseContext, ...additionalContext };
        const parts = [message];
        
        const contextEntries = Object.entries(combined)
            .filter(([, v]) => v !== undefined && v !== null && v !== "")
            .map(([k, v]) => `${k}=${v}`);

        if (contextEntries.length > 0) {
            parts.push(`(${contextEntries.join(", ")})`);
        }

        return parts.join(" ");
    }
}

module.exports = { Logger };
