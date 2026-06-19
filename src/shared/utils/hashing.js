"use strict";

const crypto = require("node:crypto");

function createUniqueKey(rawRecord) {
    // Controlio defines start_time,user_id,computer_id as the composite unique
    // cursor for the timeline report. Preserve the source timestamp text here
    // so microsecond precision is not lost before hashing.
    const material = [
        String(rawRecord.start_time ?? ""),
        String(rawRecord.user_id ?? ""),
        String(rawRecord.computer_id ?? ""),
    ].join("|");

    return crypto.createHash("sha256").update(material).digest("hex");
}

module.exports = {
    createUniqueKey,
};
