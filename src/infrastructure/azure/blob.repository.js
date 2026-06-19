"use strict";

const { AppError } = require("../../shared/errors/app-error");
const { streamToString } = require("../../shared/utils/stream");

class BlobRepository {
    constructor(blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    getContainerClient(containerName) {
        return this.blobServiceClient.getContainerClient(containerName);
    }

    async blobExists(containerName, blobPath) {
        const containerClient = this.getContainerClient(containerName);
        const blobClient = containerClient.getBlobClient(blobPath);
        return await blobClient.exists();
    }

    async readText(containerName, blobPath) {
        const containerClient = this.getContainerClient(containerName);
        const blobClient = containerClient.getBlobClient(blobPath);
        
        const exists = await blobClient.exists();
        if (!exists) {
            throw new AppError(`Blob not found: ${blobPath}`, {
                errorCode: "BLOB_NOT_FOUND",
                errorCategory: "DataQuality",
                httpStatus: 404,
            });
        }

        const response = await blobClient.download();
        return await streamToString(response.readableStreamBody, blobPath);
    }

    async readJson(containerName, blobPath) {
        const text = await this.readText(containerName, blobPath);
        try {
            return JSON.parse(text);
        } catch (error) {
            throw new AppError(`Invalid JSON in ${blobPath}: ${error.message}`, {
                errorCode: "INVALID_BRONZE_JSON",
                errorCategory: "DataQuality",
                httpStatus: 500,
            });
        }
    }

    async writeText(containerName, blobPath, content, contentType = "text/plain") {
        const containerClient = this.getContainerClient(containerName);
        await containerClient
            .getBlockBlobClient(blobPath)
            .upload(content, Buffer.byteLength(content), {
                blobHTTPHeaders: {
                    blobContentType: contentType,
                },
            });
    }

    async writeJson(containerName, blobPath, value) {
        const content = JSON.stringify(value, null, 2);
        await this.writeText(containerName, blobPath, content, "application/json");
    }

    async writeJsonLines(containerName, blobPath, records) {
        if (!records || records.length === 0) {
            return;
        }

        const content = `${records.map((item) => JSON.stringify(item)).join("\n")}\n`;
        await this.writeText(containerName, blobPath, content, "application/x-ndjson");
    }
}

module.exports = { BlobRepository };
