"use strict";

const { AppError } = require("../errors/app-error");

async function streamToString(readableStream, blobPath) {
    if (!readableStream) {
        throw new AppError(`Stream has no readable content for: ${blobPath}`, {
            errorCode: "EMPTY_BLOB_STREAM",
            errorCategory: "Transient",
            httpStatus: 503,
        });
    }

    const chunks = [];
    for await (const chunk of readableStream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString("utf8");
}

module.exports = {
    streamToString,
};
