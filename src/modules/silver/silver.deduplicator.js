"use strict";

class Deduplicator {
    constructor() {
        this.seenKeys = new Set();
        this.duplicateCount = 0;
    }

    isDuplicate(uniqueKey) {
        if (this.seenKeys.has(uniqueKey)) {
            this.duplicateCount += 1;
            return true;
        }
        this.seenKeys.add(uniqueKey);
        return false;
    }

    getDuplicateCount() {
        return this.duplicateCount;
    }
}

module.exports = { Deduplicator };
