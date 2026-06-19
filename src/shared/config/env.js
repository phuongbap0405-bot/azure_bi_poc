"use strict";

function parsePositiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
    const parsed = Number.parseInt(value ?? String(fallback), 10);
    if (!Number.isInteger(parsed) || parsed <= 0 || parsed > maximum) {
        return fallback; // For config parsing, use fallback instead of erroring here if optional
    }
    return parsed;
}

function loadConfig() {
    // Validate required variables
    const required = [
        "DATA_STORAGE_ACCOUNT",
        "BRONZE_CONTAINER",
        "SILVER_CONTAINER",
        // GOLD_CONTAINER is requested to be supported but maybe not required for silver/bronze.
        // We will validate required downstream when specifically needed, or just validate them here if we want early failure.
        // The original code did early validation in the functions.
        "BATCH_LOG_TABLE",
        "CHECKPOINT_TABLE",
        "KEY_VAULT_URL",
        "CONTROLIO_TOKEN_SECRET_NAME",
        "CONTROLIO_BASE_URL",
    ];

    const missingVariables = required.filter(name => !process.env[name]);

    if (missingVariables.length > 0) {
        throw new Error(`Missing required environment variables: ${missingVariables.join(", ")}`);
    }

    return Object.freeze({
        storageAccount: process.env.DATA_STORAGE_ACCOUNT,
        bronzeContainer: process.env.BRONZE_CONTAINER,
        silverContainer: process.env.SILVER_CONTAINER,
        goldContainer: process.env.GOLD_CONTAINER,
        batchLogTable: process.env.BATCH_LOG_TABLE,
        checkpointTable: process.env.CHECKPOINT_TABLE,
        keyVaultUrl: process.env.KEY_VAULT_URL,
        controlioTokenSecretName: process.env.CONTROLIO_TOKEN_SECRET_NAME,
        controlioBaseUrl: process.env.CONTROLIO_BASE_URL,
        schemaVersion: process.env.SCHEMA_VERSION ?? "1.0",
        codeVersion: process.env.CODE_VERSION ?? "local",
        
        controlioPageLimit: parsePositiveInteger(process.env.CONTROLIO_PAGE_LIMIT, 10000, 10000),
        maxPagesPerBatch: parsePositiveInteger(process.env.MAX_PAGES_PER_BATCH, 100, 100000),
        controlioMaxApiAttempts: parsePositiveInteger(process.env.CONTROLIO_MAX_API_ATTEMPTS, 3, 10),
        controlioRequestTimeoutMs: parsePositiveInteger(process.env.CONTROLIO_REQUEST_TIMEOUT_MS, 60000, 220000),
        
        silverRecordsPerFile: parsePositiveInteger(process.env.SILVER_RECORDS_PER_FILE, 10000, 100000),
    });
}

// Since process.env changes might be needed during test, we export the function,
// but we also export a loaded instance if we want singleton configuration.
// It's safer to load on require for standard cloud functions.
let cachedConfig = null;

function getConfig() {
    if (!cachedConfig) {
        cachedConfig = loadConfig();
    }
    return cachedConfig;
}

module.exports = {
    getConfig,
    loadConfig,
};
