# BatchLog, Checkpoint, and Sequence Diagram Design for ADF + Azure Functions

## 1. Applied Architecture

The current pipeline flow:

```text
ADF Schedule Trigger
    ↓
Bronze Azure Function Activity
    ↓
Silver Azure Function Activity
    ↓
Gold Azure Function Activity
    ↓
Pipeline Completed
```

Responsibilities of each component:

| Component | Responsibility |
|---|---|
| ADF | Scheduling, parameter passing, orchestrating Bronze → Silver → Gold, activity retry, dependency management, and monitoring |
| Azure Function | Executing business logic, calling Controlio API, processing data, and writing to ADLS |
| BatchLog | Storing the business state and processing results of each batch |
| Checkpoint | Storing the last watermark/cursor that has been safely committed to Gold |
| ADF Monitor | Storing the technical history of pipeline runs and activity runs |

ADF knows which activities succeeded or failed, but it does not know which JSON page was written to ADLS or which cursor was committed to Gold. Therefore, `BatchLog` and `Checkpoint` are still required.

### ADF Conditional Branching

When routing Bronze → Silver → Gold, ADF must use the Bronze activity's `raw_row_count` as the single source of truth. Do not use `has_data` or `coalesce`.

Use this exact If Condition expression for the Silver branch:

```adf
@greater(
    int(activity('act_bronze_controlio').output.raw_row_count),
    0
)
```

If `raw_row_count` is missing from the Bronze activity output, the pipeline should fail clearly — do not treat it as zero.

---

## 2. Who Creates `batch_id`?

### Decision

`batch_id` is generated inside the Bronze Function, not in ADF directly.

ADF passes the following technical parameters to the Bronze Function:

```json
{
  "adf_pipeline_run_id": "@pipeline().RunId",
  "adf_pipeline_name": "@pipeline().Pipeline",
  "trigger_type": "@pipeline().TriggerType",
  "window_start_utc": "2026-06-10T10:00:00Z",
  "window_end_utc": "2026-06-10T10:10:00Z",
  "resume_batch_id": null
}
```

The Bronze Function generates a deterministic `batch_id` in the following format:

```text
{pipeline_type}_{entity_name}_{trigger_time}_{short_adf_run_id}
```

Example:

```text
raw_timeline_20260610T101000Z_7f3a91c2
```

### Why does the Function generate batch_id but still use the ADF Run ID?

ADF activity retry will invoke the entire Bronze Function again. If the Function generates a random UUID on each invocation, a retry could create an additional new batch.

Therefore:

- First run: the Function generates `batch_id` from the ADF Run ID and trigger time.
- Activity retry within the same pipeline run: the same inputs produce the same `batch_id`.
- Manual resume: ADF passes `resume_batch_id` so the Function reuses the existing batch.
- Full reprocess: `resume_batch_id` is not passed; the Function creates a new batch and may store `reprocess_of_batch_id`.

### Why is it still necessary to check if the batch already exists?

ADF manages activity state but does not manage transactions inside an Azure Function.

The following scenario can occur:

```text
Function has created the BatchLog entry
Function has written some Bronze files
Function times out before returning a response to ADF
ADF marks the activity as Failed and invokes the Function again
```

Therefore, the Function must read or upsert the BatchLog by `batch_id` to ensure idempotent processing:

- Does not exist: create the batch.
- Exists and Bronze is incomplete: continue or safely reprocess.
- Exists and Bronze has already succeeded: do not reload data; return success to ADF.

This is an idempotency check, not a replacement for ADF monitoring.

---

# 3. BatchLog Table

## 3.1. Purpose

`BatchLog` stores the business state of a data batch across Bronze, Silver, and Gold.

Information specific to Azure Queue is not stored here because the current architecture no longer uses a Queue.

## 3.2. Azure Table Storage Keys

| Property | Design |
|---|---|
| Table name | `PipelineBatchLog` |
| PartitionKey | `{source_name}|{pipeline_type}|{entity_name}|{yyyyMM}` |
| RowKey | `batch_id` |

Example:

```text
PartitionKey = controlio|raw|timeline|202606
RowKey       = raw_timeline_20260610T101000Z_7f3a91c2
```

This design allows:

- Direct lookup of a batch using `PartitionKey + RowKey`.
- Filtering batches by source, pipeline type, entity, and month.
- Avoiding placing the entire history in a single partition.

## 3.3. Proposed Columns

### A. Keys and ADF Linkage

| Column | Data type | Required | Purpose | Example |
|---|---|---:|---|---|
| `PartitionKey` | String | Yes | Azure Table partition | `controlio|raw|timeline|202606` |
| `RowKey` | String | Yes | Row key, equal to `batch_id` | `raw_timeline_20260610T101000Z_7f3a91c2` |
| `batch_id` | String | Yes | Business ID of the batch | `raw_timeline_20260610T101000Z_7f3a91c2` |
| `initial_adf_pipeline_run_id` | String | Yes | ADF Run ID that created this batch initially | ADF GUID |
| `current_adf_pipeline_run_id` | String | Yes | ADF Run ID currently processing or resuming this batch | ADF GUID |
| `adf_pipeline_name` | String | Yes | Pipeline name | `pl_controlio_timeline` |
| `trigger_type` | String | Yes | Trigger type | `ScheduleTrigger`, `Manual` |
| `trigger_time_utc` | DateTime | Yes | Trigger time | `2026-06-10T10:10:00Z` |
| `run_mode` | String | Yes | Run mode | `Scheduled`, `Resume`, `Reprocess` |
| `rerun_count` | Int32 | Yes | Number of manual resume or reprocess runs for this batch | `1` |
| `reprocess_of_batch_id` | String | No | Source batch ID if this is a new reprocess batch | `raw_timeline_...` |

`ADF activity retry` does not increment `rerun_count` because it is still the same pipeline run. `rerun_count` is only incremented when a new pipeline run continues processing the same batch.

### B. Input Data Scope

| Column | Data type | Required | Purpose | Example |
|---|---|---:|---|---|
| `pipeline_type` | String | Yes | Pipeline type | `raw`, `aggregate` |
| `source_name` | String | Yes | Data source | `controlio` |
| `entity_name` | String | Yes | API or entity | `timeline` |
| `window_start_utc` | DateTime | Yes | Start of the data fetch window | `2026-06-10T10:00:00Z` |
| `window_end_utc` | DateTime | Yes | End of the data fetch window | `2026-06-10T10:10:00Z` |
| `input_cursor_prev` | String | No | Committed cursor retrieved from Checkpoint at batch start | `2026-06-10T10:00:00Z,123,456` |
| `last_persisted_cursor_prev` | String | No | Last cursor successfully written to Bronze during the current run | `2026-06-10T10:08:00Z,123,456` |
| `output_cursor_prev` | String | No | Final cursor after Bronze has completed the full batch | `2026-06-10T10:10:00Z,123,456` |
| `activity_dates_json` | String | No | List of affected dates, serialized as JSON | `["2026-06-10"]` |

Do not mix `Date` and `String` types. All timestamps should be stored as `DateTime UTC`. JSON arrays must be serialized to String when stored in Azure Table.

### C. Batch and Stage Status

| Column | Data type | Required | Purpose | Proposed Values |
|---|---|---:|---|---|
| `batch_status` | String | Yes | Overall batch status | `Created`, `Running`, `Succeeded`, `Failed`, `Cancelled`, `ManualReview` |
| `current_stage` | String | Yes | Current stage | `Bronze`, `Silver`, `Gold`, `Checkpoint`, `Completed` |
| `bronze_status` | String | Yes | Bronze stage status | `NotStarted`, `Running`, `Succeeded`, `Failed` |
| `silver_status` | String | Yes | Silver stage status | `NotStarted`, `Running`, `Succeeded`, `Failed` |
| `gold_status` | String | Yes | Gold stage status | `NotStarted`, `Running`, `DeltaCommitted`, `Succeeded`, `Failed` |
| `checkpoint_status` | String | Yes | Checkpoint commit status | `NotStarted`, `Pending`, `Succeeded`, `Failed` |

`DeltaCommitted` is critical. It indicates that the Gold Delta has been committed but the Checkpoint may not yet have been updated. If ADF retries the Gold Function, the Function will skip the MERGE and only complete the Checkpoint update.

### D. Output Paths and Data Versions

| Column | Data type | Required | Purpose | Example |
|---|---|---:|---|---|
| `bronze_path` | String | No | Raw JSON folder path | `bronze/controlio/timeline/batch_id=.../` |
| `silver_path` | String | No | Clean Parquet folder or file path | `silver/controlio/timeline/batch_id=.../` |
| `gold_table` | String | No | Gold Delta table name | `fact_timeline` |
| `gold_delta_version` | Int64 | No | Delta version after commit | `152` |
| `schema_version` | String | No | Processing schema version | `1.0` |
| `code_version` | String | No | Function or code release version | `2026.06.1` |

### E. Control Metrics

| Column | Data type | Required | Purpose | Example |
|---|---|---:|---|---|
| `api_call_count` | Int32 | Yes | Total successful or retried API calls | `3` |
| `page_count` | Int32 | Yes | Number of pages or chunks persisted | `2` |
| `raw_row_count` | Int64 | Yes | Records written to Bronze | `10000` |
| `clean_row_count` | Int64 | Yes | Clean records in Silver | `9950` |
| `rejected_row_count` | Int64 | Yes | Invalid or error records | `50` |
| `insert_count` | Int64 | Yes | Records inserted into Gold | `9000` |
| `update_count` | Int64 | Yes | Records updated in Gold | `500` |
| `skip_count` | Int64 | Yes | Records unchanged in Gold | `450` |

All counts should be initialized to `0` rather than null for easier querying and aggregation.

### F. Error Information

| Column | Data type | Required | Purpose | Example |
|---|---|---:|---|---|
| `failed_stage` | String | No | Stage where failure occurred | `Bronze`, `Silver`, `Gold`, `Checkpoint` |
| `error_category` | String | No | Error category | `Transient`, `Configuration`, `DataQuality`, `Concurrency`, `Permanent` |
| `error_code` | String | No | Short error code | `CONTROLIO_429`, `CHECKPOINT_412` |
| `error_message` | String | No | Brief error description | `Controlio API rate limit exceeded` |
| `error_details_ref` | String | No | Link or correlation ID to App Insights or detailed log | `appinsights:operation-id` |

Do not store long stack traces directly in Azure Table. Store only enough information to locate detailed logs in Application Insights.

### G. Timestamps

| Column | Data type | Required | Purpose |
|---|---|---:|---|
| `created_at` | DateTime | Yes | Time the batch was created |
| `started_at` | DateTime | No | Time processing started |
| `bronze_started_at` | DateTime | No | Time Bronze processing started |
| `bronze_finished_at` | DateTime | No | Time Bronze processing completed |
| `silver_started_at` | DateTime | No | Time Silver processing started |
| `silver_finished_at` | DateTime | No | Time Silver processing completed |
| `gold_started_at` | DateTime | No | Time Gold processing started |
| `gold_committed_at` | DateTime | No | Time Delta commit succeeded |
| `checkpoint_updated_at` | DateTime | No | Time Checkpoint commit succeeded |
| `finished_at` | DateTime | No | Time the full batch completed |
| `failed_at` | DateTime | No | Time of the most recent failure |
| `updated_at` | DateTime | Yes | Time of the most recent update |

---

# 4. Checkpoint Table

## 4.1. Purpose

Checkpoint stores only the state that has been safely committed to Gold.

The cursor must not be written to Checkpoint immediately after Bronze or Silver.

Rules:

```text
Bronze success
    → Checkpoint not updated

Silver success
    → Checkpoint not updated

Gold Delta commit success
    → Checkpoint updated

Checkpoint update success
    → batch_status = Succeeded
```

## 4.2. Azure Table Storage Keys

| Property | Design |
|---|---|
| Table name | `PipelineCheckpoint` |
| PartitionKey | `{source_name}|{pipeline_type}` |
| RowKey | `entity_name` |

Example:

```text
PartitionKey = controlio|raw
RowKey       = timeline
```

Each source + pipeline type + entity has exactly one active checkpoint.

## 4.3. Proposed Columns

| Column | Data type | Required | Purpose | Example |
|---|---|---:|---|---|
| `PartitionKey` | String | Yes | Checkpoint partition | `controlio|raw` |
| `RowKey` | String | Yes | Entity checkpoint key | `timeline` |
| `checkpoint_id` | String | Yes | Human-readable ID | `controlio_raw_timeline` |
| `pipeline_type` | String | Yes | Pipeline type | `raw` |
| `source_name` | String | Yes | Data source | `controlio` |
| `entity_name` | String | Yes | Entity or API | `timeline` |
| `watermark_type` | String | Yes | Watermark type | `Cursor`, `Timestamp`, `Date` |
| `is_initialized` | Boolean | Yes | Whether a successful checkpoint exists | `true` |
| `committed_window_start_utc` | DateTime | No | Window start of the last batch committed to Gold | `2026-06-10T10:00:00Z` |
| `committed_window_end_utc` | DateTime | No | Window end of the last batch committed to Gold | `2026-06-10T10:10:00Z` |
| `committed_cursor_prev` | String | No | Last cursor committed to Gold | `2026-06-10T10:10:00Z,123,456` |
| `committed_date` | DateTime | No | Date-based watermark for aggregate pipelines | `2026-06-10T00:00:00Z` |
| `last_success_batch_id` | String | No | Last batch ID that successfully updated this checkpoint | `raw_timeline_...` |
| `last_success_adf_pipeline_run_id` | String | No | ADF Run ID of the last successful batch | ADF GUID |
| `last_success_at` | DateTime | No | Time the last batch completed successfully | `2026-06-10T10:12:00Z` |
| `checkpoint_version` | Int64 | Yes | Logical version for audit purposes | `152` |
| `updated_at` | DateTime | Yes | Time this checkpoint was last updated | `2026-06-10T10:12:05Z` |

Azure Table provides a system-managed `ETag`. The Gold Function must update the Checkpoint using optimistic concurrency:

1. Read the Checkpoint and receive the ETag.
2. Commit the Gold Delta.
3. Update the Checkpoint with the ETag condition.
4. If the ETag no longer matches, Azure Table returns `412 Precondition Failed`.
5. The Function re-reads the Checkpoint:
   - If it already points to the current batch: treat as idempotent success.
   - If it points to a different batch: flag as a concurrency conflict and do not overwrite blindly.

---

# 5. Sequence Diagram — Bronze

Link: https://mermaid.ai/d/c785a262-6091-4901-8e8c-2cc5c47073a4

```mermaid
sequenceDiagram
    autonumber

    participant ADF as ADF Pipeline
    participant BF as Bronze Function
    participant BL as BatchLog
    participant CP as Checkpoint
    participant API as Controlio API
    participant BZ as ADLS Bronze

    ADF->>BF: Invoke with RunId, window and resume_batch_id

    alt resume_batch_id is provided
        BF->>BF: Use existing batch_id
    else New scheduled run
        BF->>BF: Generate deterministic batch_id from RunId
    end

    BF->>BL: Get entity by PartitionKey + batch_id

    alt Batch does not exist
        BF->>BL: Insert batch with status Created
    else bronze_status = Succeeded
        BL-->>BF: Return existing Bronze result
        BF-->>ADF: Success with existing batch_id
    else Batch exists but Bronze is incomplete
        BF->>BL: Update current RunId and set Bronze Running
    end

    BF->>CP: Read committed Checkpoint
    CP-->>BF: committed_cursor_prev + ETag

    BF->>BL: Save input_cursor_prev and window
    BF->>BL: Set batch_status = Running
    BF->>BL: Set current_stage = Bronze
    BF->>BL: Set bronze_status = Running

    loop While Controlio returns another page
        BF->>API: Request data using current prev

        alt API success
            API-->>BF: JSON records + next prev
        else Timeout / 429 / 5xx
            BF->>API: Internal retry with backoff

            alt Internal retry succeeds
                API-->>BF: JSON records + next prev
            else Internal retry exhausted
                BF->>BL: Set Bronze Failed
                BF->>BL: Save error category, code and message
                BF-->>ADF: Return failure
            end
        end

        BF->>BZ: Write deterministic page file
        BZ-->>BF: Write success
        BF->>BL: Increase page_count and raw_row_count
        BF->>BL: Save last_persisted_cursor_prev
    end

    BF->>BL: Save output_cursor_prev and bronze_path
    BF->>BL: Set bronze_status = Succeeded
    BF->>BL: Set current_stage = Silver
    BF-->>ADF: Return JObject with batch_id

    alt Bronze activity failed
        ADF->>BF: Retry entire Bronze activity

        Note over ADF,BF: Same pipeline RunId produces the same batch_id

        alt Retry succeeds
            BF-->>ADF: Success
        else ADF retry exhausted
            ADF-->>ADF: Pipeline run Failed
        end
    end

    Note over CP: Checkpoint is not updated in Bronze
```

---

# 6. Sequence Diagram — Silver

Link: https://mermaid.ai/d/9ee2a4a3-f390-4700-8a1f-36ac69516ac4

```mermaid
sequenceDiagram
    autonumber

    participant ADF as ADF Pipeline
    participant SF as Silver Function
    participant BL as BatchLog
    participant BZ as ADLS Bronze
    participant SV as ADLS Silver

    ADF->>SF: Invoke with batch_id and current RunId
    SF->>BL: Read BatchLog by batch_id
    BL-->>SF: Batch state and bronze_path

    alt silver_status = Succeeded
        SF-->>ADF: Return idempotent success
    else bronze_status is not Succeeded
        SF->>BL: Set Silver Failed
        SF->>BL: error_category = Permanent
        SF-->>ADF: Return dependency failure
    else Bronze is ready
        SF->>BL: Set batch_status = Running
        SF->>BL: Set current_stage = Silver
        SF->>BL: Set silver_status = Running

        SF->>BZ: Read Bronze files

        alt Temporary storage error
            SF->>BZ: Internal retry read
        end

        BZ-->>SF: Raw records

        SF->>SF: Validate schema
        SF->>SF: Normalize data types
        SF->>SF: Generate unique_key
        SF->>SF: Remove duplicates
        SF->>SF: Split valid and rejected records

        alt Schema or business validation fails
            SF->>BL: Set Silver Failed
            SF->>BL: error_category = DataQuality
            SF->>BL: Save error details reference
            SF-->>ADF: Return non-retryable failure
        else Transformation succeeds
            SF->>SV: Replace deterministic Silver output by batch_id

            alt Silver write succeeds
                SV-->>SF: Write success
                SF->>BL: Save silver_path
                SF->>BL: Save clean and rejected row counts
                SF->>BL: Set silver_status = Succeeded
                SF->>BL: Set current_stage = Gold
                SF-->>ADF: Return success with batch_id
            else Silver write fails
                SF->>BL: Set Silver Failed
                SF->>BL: Save storage error
                SF-->>ADF: Return failure
            end
        end
    end

    alt Silver activity failed with retryable error
        ADF->>SF: Retry entire Silver activity

        Note over SF,SV: Same batch_id and deterministic output prevent duplicate files

        alt Retry succeeds
            SF-->>ADF: Success
        else ADF retry exhausted
            ADF-->>ADF: Pipeline run Failed
        end
    end

    Note over BL: Checkpoint is not updated in Silver
```

---

# 7. Sequence Diagram — Gold and Checkpoint

Link: https://mermaid.ai/d/ea0cf6a3-371d-4dc6-9976-ac4a9770da94

```mermaid
sequenceDiagram
    autonumber

    participant ADF as ADF Pipeline
    participant GF as Gold Function
    participant BL as BatchLog
    participant SV as ADLS Silver
    participant GD as ADLS Gold Delta
    participant DL as Delta Log
    participant CP as Checkpoint

    ADF->>GF: Invoke with batch_id and current RunId
    GF->>BL: Read BatchLog by batch_id
    BL-->>GF: silver_status, silver_path, output_cursor_prev, gold_status, checkpoint_status

    alt gold_status = Succeeded and checkpoint_status = Succeeded
        GF-->>ADF: Return idempotent success

    else gold_status = DeltaCommitted
        Note over GF,BL: Delta already committed in a previous attempt
        GF->>CP: Read current Checkpoint + ETag
        GF->>CP: Retry only Checkpoint update

        alt Checkpoint update succeeds
            GF->>BL: Set checkpoint_status = Succeeded
            GF->>BL: Set gold_status = Succeeded
            GF->>BL: Set batch_status = Succeeded
            GF->>BL: Set current_stage = Completed
            GF-->>ADF: Return success
        else Checkpoint conflict or failure
            GF->>BL: Set checkpoint_status = Failed
            GF->>BL: Save checkpoint error
            GF-->>ADF: Return failure
        end

    else silver_status is not Succeeded
        GF->>BL: Set Gold Failed
        GF->>BL: Save dependency error
        GF-->>ADF: Return failure

    else Gold has not committed
        GF->>BL: Set batch_status = Running
        GF->>BL: Set current_stage = Gold
        GF->>BL: Set gold_status = Running
        GF->>BL: Set checkpoint_status = NotStarted

        GF->>CP: Read Checkpoint and ETag
        CP-->>GF: Current committed watermark + ETag

        GF->>SV: Read clean Silver data
        SV-->>GF: Clean records

        GF->>GD: MERGE by unique_key and activity_date
        Note over GF,GD: New → INSERT<br/>Changed → UPDATE<br/>Same → SKIP

        alt Delta merge or commit fails
            GD-->>GF: Failure
            GF->>BL: Set gold_status = Failed
            GF->>BL: Save merge/commit error
            GF-->>ADF: Return failure

        else Delta commit succeeds
            GD->>DL: Commit new Delta version
            DL-->>GF: Delta version
            GF->>BL: Save insert, update and skip counts
            GF->>BL: Save gold_delta_version
            GF->>BL: Set gold_status = DeltaCommitted
            GF->>BL: Set checkpoint_status = Pending

            GF->>CP: Conditional update using ETag
            Note over GF,CP: Commit output_cursor_prev, window and batch_id

            alt Checkpoint update succeeds
                CP-->>GF: New ETag/version
                GF->>BL: Set checkpoint_status = Succeeded
                GF->>BL: Set gold_status = Succeeded
                GF->>BL: Set batch_status = Succeeded
                GF->>BL: Set current_stage = Completed
                GF->>BL: Set checkpoint_updated_at and finished_at
                GF-->>ADF: Return success

            else ETag conflict
                CP-->>GF: HTTP 412 Precondition Failed
                GF->>CP: Read latest Checkpoint

                alt Checkpoint already points to this batch
                    GF->>BL: Mark idempotent success
                    GF-->>ADF: Return success
                else Checkpoint points to another batch
                    GF->>BL: Set checkpoint_status = Failed
                    GF->>BL: error_category = Concurrency
                    GF->>BL: Set batch_status = ManualReview
                    GF-->>ADF: Return failure
                end

            else Other Checkpoint write failure
                GF->>BL: Keep gold_status = DeltaCommitted
                GF->>BL: Set checkpoint_status = Failed
                GF->>BL: Save checkpoint error
                GF-->>ADF: Return failure
            end
        end
    end

    alt Gold activity failed with retryable error
        ADF->>GF: Retry entire Gold activity

        Note over GF,BL: If DeltaCommitted, retry skips MERGE and only updates Checkpoint

        alt Retry succeeds
            GF-->>ADF: Success
        else ADF retry exhausted
            ADF-->>ADF: Pipeline run Failed
        end
    end
```

---

# 8. ADF Pipeline Control Flow

```mermaid
flowchart LR
    T[Schedule Trigger] --> B[Bronze Function Activity]

    B -->|Upon Success| S[Silver Function Activity]
    S -->|Upon Success| G[Gold Function Activity]
    G -->|Upon Success| OK[Pipeline Completed]

    B -->|Upon Failure| F[Failure Handling]
    S -->|Upon Failure| F
    G -->|Upon Failure| F

    F --> X[Fail Activity]
```

The failure-handling branch must end with a `Fail Activity`. Without it, ADF may report the pipeline as Succeeded even though a data processing activity has failed.

---

# 9. Retry Rules

## Function-Level Retry

Retry only on transient errors:

- HTTP 429
- HTTP 500, 502, 503, 504
- Network timeout
- ADLS temporarily unavailable
- Checkpoint write temporary failure

Do not retry on permanent errors:

- Invalid API token
- Misconfiguration
- Invalid schema
- Missing required fields
- Business validation failure

## ADF Activity Retry

ADF retries the entire Azure Function Activity.

Recommended initial settings:

```text
Retry count: 2 or 3
Retry interval: 60–120 seconds
```

The Function must be idempotent because each retry is a full re-invocation of the Function.

---

# 10. Azure Function Activity Timeout

Azure Function Activity invokes the Function via HTTP. The Function must return a response within the HTTP invocation timeout limit.

If Bronze, Silver, or Gold may run longer than a few minutes, one of the following approaches should be used:

1. Split the batch into smaller units so each Function completes within the time limit.
2. Use Durable Functions with an async pattern and allow ADF to poll for the status.

The `BatchLog` and `Checkpoint` design in this document applies to both synchronous Functions and Durable Functions.

---

# 11. Design Summary

## BatchLog

Stores:

- Which data batch is being processed.
- Input cursor, last persisted cursor, and output cursor.
- Bronze and Silver paths, and Gold Delta version.
- Row counts and merge results.
- Business state for resume and reprocess operations.
- `DeltaCommitted` status to handle failures between Gold commit and Checkpoint update.

## Checkpoint

Stores the watermark that has been safely committed after Gold.

## ADF

ADF manages:

- Scheduling
- Activity dependency
- Activity retry
- Pipeline and activity monitoring
- Failure branch
- Manual rerun

## Azure Function

The Function manages:

- Creating or reusing `batch_id`
- Idempotency
- API pagination using `prev`
- Bronze, Silver, and Gold processing
- BatchLog updates
- Checkpoint commit