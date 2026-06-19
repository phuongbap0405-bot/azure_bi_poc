"use strict";

const { AppError } = require("../../shared/errors/app-error");

function buildCursorFromLastRow(rows) {
    if (!Array.isArray(rows) || rows.length === 0) {
        return null;
    }

    const last = rows[rows.length - 1];
    const cursorStart = last.start_time ?? last.start;
    const userId = last.user_id ?? last.userId ?? last.user?.id;
    const computerId = last.computer_id ?? last.computerId ?? last.computer?.id;

    if (
        cursorStart === undefined ||
        cursorStart === null ||
        userId === undefined ||
        userId === null ||
        computerId === undefined ||
        computerId === null
    ) {
        throw new AppError(
            `Cannot build prev cursor from the final record. Available fields: ${Object.keys(last).join(", ")}`,
            {
                errorCode: "CURSOR_FIELDS_MISSING",
                errorCategory: "DataQuality",
                httpStatus: 500,
            }
        );
    }

    return `${cursorStart},${userId},${computerId}`;
}

module.exports = {
    buildCursorFromLastRow,
};
