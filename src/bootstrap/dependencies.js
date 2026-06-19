"use strict";

const { DefaultAzureCredential } = require("@azure/identity");
const { getConfig } = require("../shared/config/env");
const { Logger } = require("../shared/logging/logger");
const { AzureClientsFactory } = require("../infrastructure/azure/azure-clients");
const { TableRepository } = require("../infrastructure/azure/table.repository");
const { BlobRepository } = require("../infrastructure/azure/blob.repository");
const { KeyVaultRepository } = require("../infrastructure/azure/key-vault.repository");
const { ControlioClient } = require("../infrastructure/controlio/controlio.client");
const { BronzeService } = require("../modules/bronze/bronze.service");
const { SilverService } = require("../modules/silver/silver.service");

// Singleton instances for reuse across function invocations
let credentialInstance = null;
let azureClientsFactory = null;

function getCredential() {
    if (!credentialInstance) {
        credentialInstance = new DefaultAzureCredential();
    }
    return credentialInstance;
}

function getAzureClientsFactory() {
    if (!azureClientsFactory) {
        azureClientsFactory = new AzureClientsFactory(getCredential());
    }
    return azureClientsFactory;
}

function resolveDependencies(context) {
    const config = getConfig();
    const logger = new Logger(context);
    const factory = getAzureClientsFactory();

    // Infrastructure
    const batchLogClient = factory.getTableClient(config.storageAccount, config.batchLogTable);
    const checkpointClient = factory.getTableClient(config.storageAccount, config.checkpointTable);
    const blobServiceClient = factory.getBlobServiceClient(config.storageAccount);
    const secretClient = factory.getSecretClient(config.keyVaultUrl);

    // Repositories
    const batchLogRepository = new TableRepository(batchLogClient);
    const checkpointRepository = new TableRepository(checkpointClient);
    const blobRepository = new BlobRepository(blobServiceClient);
    const keyVaultRepository = new KeyVaultRepository(secretClient);
    const controlioClient = new ControlioClient();

    // Services
    const bronzeService = new BronzeService({
        config,
        logger,
        batchLogRepository,
        checkpointRepository,
        blobRepository,
        keyVaultRepository,
        controlioClient,
    });

    const silverService = new SilverService({
        config,
        logger,
        batchLogRepository,
        blobRepository,
    });

    return {
        config,
        logger,
        bronzeService,
        silverService,
        batchLogRepository, // useful if wrapper needs to log failure outside service
    };
}

module.exports = {
    resolveDependencies,
};
