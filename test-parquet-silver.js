"use strict";
// Standalone test for Silver Parquet output - run from project root:
//   node test-parquet-silver.js

const parquet = require("parquetjs-lite");
const fs = require("fs");
const crypto = require("crypto");

// ── replicate the exact schema & helper from silver.service.js ────────────────

const PARQUET_SCHEMA = new parquet.ParquetSchema({
    unique_key:          { type: "UTF8",    compression: "SNAPPY" },
    batch_id:            { type: "UTF8",    compression: "SNAPPY" },
    source_page_number:  { type: "INT32",   optional: true, compression: "SNAPPY" },
    source_name:         { type: "UTF8",    optional: true, compression: "SNAPPY" },
    entity_name:         { type: "UTF8",    optional: true, compression: "SNAPPY" },
    activity:            { type: "BOOLEAN", optional: true, compression: "SNAPPY" },
    activity_id:         { type: "INT32",   optional: true, compression: "SNAPPY" },
    activity_type:       { type: "INT32",   optional: true, compression: "SNAPPY" },
    activity_name:       { type: "UTF8",    optional: true, compression: "SNAPPY" },
    caption:             { type: "UTF8",    optional: true, compression: "SNAPPY" },
    user_id:             { type: "INT32",   optional: true, compression: "SNAPPY" },
    computer_id:         { type: "INT32",   optional: true, compression: "SNAPPY" },
    start_time_utc:      { type: "UTF8",    optional: true, compression: "SNAPPY" },
    end_time_utc:        { type: "UTF8",    optional: true, compression: "SNAPPY" },
    activity_date:       { type: "UTF8",    optional: true, compression: "SNAPPY" },
    duration_seconds:    { type: "DOUBLE",  optional: true, compression: "SNAPPY" },
    is_website:          { type: "BOOLEAN", optional: true, compression: "SNAPPY" },
    url_domain:          { type: "UTF8",    optional: true, compression: "SNAPPY" },
    ingested_at_utc:     { type: "UTF8",    optional: true, compression: "SNAPPY" },
});

const TIMESTAMP_FIELDS = new Set(["start_time_utc", "end_time_utc", "ingested_at_utc"]);

const cleanParquetRecord = (record) => {
    const cleaned = {};
    for (const [key, value] of Object.entries(record)) {
        if (value === null || value === undefined) continue;
        if (TIMESTAMP_FIELDS.has(key)) {
            cleaned[key] = value instanceof Date ? value.toISOString() : String(value);
        } else {
            cleaned[key] = value;
        }
    }
    return cleaned;
};

// ── helpers ───────────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function assert(condition, msg) {
    if (condition) {
        console.log(`  ✅  ${msg}`);
        passed++;
    } else {
        console.error(`  ❌  ${msg}`);
        failed++;
    }
}

async function writeAndRead(rows) {
    const tmpFile = `/tmp/${crypto.randomUUID()}.parquet`;
    try {
        const writer = await parquet.ParquetWriter.openFile(PARQUET_SCHEMA, tmpFile);
        for (const row of rows) {
            await writer.appendRow(cleanParquetRecord(row));
        }
        await writer.close();

        const buffer = await fs.promises.readFile(tmpFile);
        assert(buffer.length > 0, "Parquet buffer has bytes");

        const reader = await parquet.ParquetReader.openFile(tmpFile);
        const cursor = reader.getCursor();
        const results = [];
        let rec;
        while ((rec = await cursor.next())) results.push(rec);
        await reader.close();
        return results;
    } finally {
        await fs.promises.unlink(tmpFile).catch(() => { });
    }
}

// ── test cases ────────────────────────────────────────────────────────────────

async function testZeroRecords() {
    console.log("\n[1] 0 records — write valid empty Parquet");
    const rows = await writeAndRead([]);
    assert(rows.length === 0, "Row count = 0");
}

async function testOneRecord() {
    console.log("\n[2] 1 record — basic roundtrip");
    const rows = await writeAndRead([{
        unique_key: "key-001",
        batch_id:   "batch-001",
        source_page_number: 1,
        source_name: "controlio",
        entity_name: "timeline",
        activity: true,
        activity_id: 1,
        activity_type: 1,
        activity_name: "Chrome",
        caption: "Google",
        user_id: 1,
        computer_id: 1,
        start_time_utc: "2024-01-15T08:00:00.000Z",
        end_time_utc: "2024-01-15T08:30:00.000Z",
        activity_date: "2024-01-15",
        duration_seconds: 1800,
        is_website: false,
        url_domain: null,
        ingested_at_utc: "2024-01-16T00:00:00.000Z",
    }]);
    assert(rows.length === 1, "Row count = 1");
    assert(rows[0].unique_key === "key-001", "unique_key matches");
    assert(rows[0].batch_id === "batch-001", "batch_id matches");
    assert(rows[0].start_time_utc === "2024-01-15T08:00:00.000Z", "start_time_utc is ISO string");
    assert(rows[0].is_website === false, "is_website is boolean false");
    assert(rows[0].duration_seconds === 1800, "duration_seconds is number");
    assert(!("url_domain" in rows[0]) || rows[0].url_domain === null, "null url_domain is absent or null");
}

async function testManyRecords() {
    console.log("\n[3] many records — 500 rows");
    const inputs = Array.from({ length: 500 }, (_, i) => ({
        unique_key: `key-${i}`,
        batch_id:   "batch-many",
        source_page_number: Math.floor(i / 50) + 1,
        activity: i % 2 === 0,
        duration_seconds: i * 1.5,
        is_website: i % 2 === 0,
        ingested_at_utc: new Date().toISOString(),
    }));
    const rows = await writeAndRead(inputs);
    assert(rows.length === 500, "Row count = 500");
}

async function testNullFields() {
    console.log("\n[4] null fields — nulls omitted from optional columns");
    const rows = await writeAndRead([{
        unique_key: "key-null",
        batch_id: "batch-null",
        source_name: null,
        entity_name: undefined,
        activity: null,
        is_website: null,
        duration_seconds: null,
        start_time_utc: null,
    }]);
    assert(rows.length === 1, "Row count = 1");
    // optional nulls should come back as null or undefined
    const r = rows[0];
    assert(r.source_name == null, "source_name is null");
    assert(r.activity == null, "activity is null");
    assert(r.is_website == null, "is_website is null");
    assert(r.duration_seconds == null, "duration_seconds is null");
    assert(r.start_time_utc == null, "start_time_utc is null");
}

async function testTimestamps() {
    console.log("\n[5] timestamps — ISO string preserved, Date object normalised");
    const now = new Date("2024-06-01T12:34:56.789Z");
    const rows = await writeAndRead([{
        unique_key: "key-ts",
        batch_id: "batch-ts",
        start_time_utc: now,                          // Date object input
        end_time_utc: "2024-06-01T13:00:00.000Z",    // string input
        ingested_at_utc: "2024-06-01T00:00:00Z",
    }]);
    assert(rows.length === 1, "Row count = 1");
    assert(rows[0].start_time_utc === now.toISOString(), "Date object → ISO string");
    assert(rows[0].end_time_utc === "2024-06-01T13:00:00.000Z", "ISO string preserved");
}

async function testBoolean() {
    console.log("\n[6] boolean — true and false");
    const rows = await writeAndRead([
        { unique_key: "k-true", batch_id: "b", is_website: true },
        { unique_key: "k-false", batch_id: "b", is_website: false },
    ]);
    assert(rows.length === 2, "Row count = 2");
    assert(rows[0].is_website === true, "is_website = true");
    assert(rows[1].is_website === false, "is_website = false");
}

async function testDurationSeconds() {
    console.log("\n[7] duration_seconds — integer and float");
    const rows = await writeAndRead([
        { unique_key: "k-int", batch_id: "b", duration_seconds: 3600 },
        { unique_key: "k-float", batch_id: "b", duration_seconds: 123.456 },
    ]);
    assert(rows.length === 2, "Row count = 2");
    assert(rows[0].duration_seconds === 3600, "integer duration preserved");
    assert(Math.abs(rows[1].duration_seconds - 123.456) < 1e-6, "float duration preserved");
}

async function testDuplicates() {
    console.log("\n[8] duplicates — same unique_key written once (caller deduplicates, but verify schema)");
    // The Deduplicator lives in the service; here we just confirm the file accepts
    // two separate rows if they were already deduplicated upstream.
    const rows = await writeAndRead([
        { unique_key: "k-dup", batch_id: "b", activity: true  },
        { unique_key: "k-dup", batch_id: "b", activity: false }, // would be filtered by Deduplicator in prod
    ]);
    assert(rows.length === 2, "Both rows written (Parquet layer does not deduplicate)");
}

async function testUnicode() {
    console.log("\n[9] Unicode — CJK, emoji, Arabic");
    const rows = await writeAndRead([{
        unique_key: "key-unicode",
        batch_id: "batch-u",
        caption: "テスト 테스트 测试 🚀",
        activity_name: "مرحبا",
        source_name: "controlio",
    }]);
    assert(rows.length === 1, "Row count = 1");
    assert(rows[0].caption === "テスト 테스트 测试 🚀", "CJK + emoji preserved");
    assert(rows[0].activity_name === "مرحبا", "Arabic preserved");
}

// ── runner ────────────────────────────────────────────────────────────────────

async function main() {
    console.log("=== Silver Parquet test suite ===");
    await testZeroRecords();
    await testOneRecord();
    await testManyRecords();
    await testNullFields();
    await testTimestamps();
    await testBoolean();
    await testDurationSeconds();
    await testDuplicates();
    await testUnicode();

    console.log(`\n${"=".repeat(40)}`);
    console.log(`Passed: ${passed}  |  Failed: ${failed}`);
    if (failed > 0) process.exit(1);
}

main().catch((err) => { console.error(err); process.exit(1); });
