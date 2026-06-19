"use strict";

class KeyVaultRepository {
    constructor(secretClient) {
        this.secretClient = secretClient;
    }

    async getSecretValue(secretName) {
        const secret = await this.secretClient.getSecret(secretName);
        return secret?.value || null;
    }
}

module.exports = { KeyVaultRepository };
