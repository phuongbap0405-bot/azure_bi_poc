"use strict";

const { app } = require("@azure/functions");
const { resolveDependencies } = require("../bootstrap/dependencies");
const { buildErrorResponse } = require("../shared/errors/error-response");
const { AppError } = require("../shared/errors/app-error");

app.http("Bronze", {
    methods: ["POST"],
    authLevel: "function",

    handler: async (request, context) => {
        let dependencies;
        try {
            dependencies = resolveDependencies(context);
        } catch (error) {
            context.error("Bronze Function failed during initialization", error);
            const appError = error instanceof AppError ? error : new AppError(error.message, { httpStatus: 500, errorCategory: "Configuration" });
            return buildErrorResponse(appError);
        }

        const { bronzeService, logger } = dependencies;

        try {
            let body;
            try {
                body = await request.json();
            } catch (error) {
                return buildErrorResponse(
                    new AppError(`Request body must be valid JSON: ${error.message}`, {
                        errorCode: "INVALID_JSON_BODY",
                        errorCategory: "DataQuality",
                        httpStatus: 400,
                    })
                );
            }

            return await bronzeService.process(body, context.invocationId);
        } catch (error) {
            const appError = error instanceof AppError ? error : new AppError(error.message, { httpStatus: 500 });
            return buildErrorResponse(appError);
            // batch log failure state is updated inside the service where it has access to the parsed batch_id.
        }
    },
});
