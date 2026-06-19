"use strict";

class TableRepository {
    constructor(tableClient) {
        this.tableClient = tableClient;
    }

    async getEntity(partitionKey, rowKey) {
        try {
            return await this.tableClient.getEntity(partitionKey, rowKey);
        } catch (error) {
            if (error.statusCode === 404) {
                return null;
            }
            throw error;
        }
    }

    async upsertEntity(entity, mode = "Merge") {
        await this.tableClient.upsertEntity(entity, mode);
    }

    async getBatch(partitionKey, rowKey) {
        return await this.getEntity(partitionKey, rowKey);
    }

    async getCheckpoint(partitionKey, rowKey) {
        return await this.getEntity(partitionKey, rowKey);
    }

    async upsertBatch(entity) {
        await this.upsertEntity(entity, "Merge");
    }

    async upsertCheckpoint(entity) {
        await this.upsertEntity(entity, "Merge");
    }

    // specific status updates could go here, but since the update fields are
    // dynamic (started_at, failed_at, different stages, row counts),
    // it's easier to expose `upsertBatch` and let the service pass the partial object.
}

module.exports = { TableRepository };
