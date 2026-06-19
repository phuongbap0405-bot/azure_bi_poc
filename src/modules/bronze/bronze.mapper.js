"use strict";

const { AppError } = require("../../shared/errors/app-error");
const { validateDateRange, parseOptionalUtcDate } = require("../../shared/utils/date");
const {
    validateLogicalName,
    normalizeCommaSeparatedIds,
    normalizeActivityType,
} = require("../../shared/utils/validation");
const constants = require("./bronze.constants");

function parseRequestPayload(body) {
    if (!body || typeof body !== "object") {
        throw new AppError("Request body must be valid JSON", {
            errorCode: "INVALID_JSON_BODY",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const runId = body.adf_pipeline_run_id;
    if (!runId) {
        throw new AppError("adf_pipeline_run_id is required", {
            errorCode: "MISSING_RUN_ID",
            errorCategory: "DataQuality",
            httpStatus: 400,
        });
    }

    const windowStartUtcText = body.window_start_utc ?? null;
    const windowEndUtcText = body.window_end_utc ?? null;
    const startTime = body.start_time ?? windowStartUtcText?.slice(0, 10) ?? null;
    const endTime = body.end_time ?? windowEndUtcText?.slice(0, 10) ?? null;
    
    validateDateRange(startTime, endTime);

    const sourceName = body.source_name ?? constants.DEFAULT_SOURCE_NAME;
    const pipelineType = body.pipeline_type ?? constants.DEFAULT_PIPELINE_TYPE;
    const entityName =
        body.entity_name ??
        process.env.CONTROLIO_ENTITY_NAME ??
        constants.DEFAULT_ENTITY_NAME;

    validateLogicalName(sourceName, "source_name");
    validateLogicalName(pipelineType, "pipeline_type");
    validateLogicalName(entityName, "entity_name");

    const triggerTimeUtcText =
        body.trigger_time_utc ??
        windowEndUtcText ??
        new Date().toISOString();

    const triggerTimeUtc = parseOptionalUtcDate(triggerTimeUtcText, "trigger_time_utc");
    const windowStartUtc = parseOptionalUtcDate(windowStartUtcText, "window_start_utc");
    const windowEndUtc = parseOptionalUtcDate(windowEndUtcText, "window_end_utc");

    const activities = normalizeCommaSeparatedIds(body.activities, "activities");
    const users = normalizeCommaSeparatedIds(body.users, "users");
    const departments = normalizeCommaSeparatedIds(body.departments, "departments");
    const activityType = normalizeActivityType(body.activity_type);
    const stringToMatch =
        body.string_to_match === undefined || body.string_to_match === null
            ? null
            : String(body.string_to_match);

    return {
        runId,
        startTime,
        endTime,
        sourceName,
        pipelineType,
        entityName,
        triggerTimeUtc,
        triggerTimeUtcText,
        windowStartUtc,
        windowStartUtcText,
        windowEndUtc,
        windowEndUtcText,
        activities,
        users,
        departments,
        activityType,
        stringToMatch,
        resumeBatchId: body.resume_batch_id || null,
        adfPipelineName: body.adf_pipeline_name ?? "pl_controlio_timeline",
        triggerType: body.trigger_type ?? "Manual",
        runMode: body.run_mode,
    };
}

module.exports = {
    parseRequestPayload,
};
