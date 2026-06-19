"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { TableClient } = require("@azure/data-tables");
const { SecretClient } = require("@azure/keyvault-secrets");

class AzureClientsFactory {
    constructor(credential) {
        this.credential = credential;
        this.clients = {};
    }

    getBlobServiceClient(storageAccount) {
        const url = `https://${storageAccount}.blob.core.windows.net`;
        if (!this.clients[url]) {
            this.clients[url] = new BlobServiceClient(url, this.credential);
        }
        return this.clients[url];
    }

    getTableClient(storageAccount, tableName) {
        const url = `https://${storageAccount}.table.core.windows.net`;
        const key = `${url}|${tableName}`;
        if (!this.clients[key]) {
            this.clients[key] = new TableClient(url, tableName, this.credential);
        }
        return this.clients[key];
    }

    getSecretClient(keyVaultUrl) {
        if (!this.clients[keyVaultUrl]) {
            this.clients[keyVaultUrl] = new SecretClient(keyVaultUrl, this.credential);
        }
        return this.clients[keyVaultUrl];
    }
}

module.exports = { AzureClientsFactory };
