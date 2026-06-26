# Bronze Function - Data Retrieval Explanation

## Overview

Bronze function là bước đầu tiên trong pipeline extract từ hệ thống Controlio. Nó có trách nhiệm lấy dữ liệu raw từ Controlio API và lưu trữ vào Azure Blob Storage dưới dạng JSON files được phân trang.

## Data Source

### API Endpoint
- **Source**: Controlio API
- **Endpoint**: `/api/v1/statistics/timeline`
- **Protocol**: HTTPS (Bearer Token authentication)
- **Token**: Lấy từ Azure Key Vault (secret name: `CONTROLIO_TOKEN_SECRET_NAME`)

### Storage Destination
- **Type**: Azure Blob Storage
- **Container**: `BRONZE_CONTAINER` (từ environment variables)
- **Path Structure**:
  ```
  {sourceName}/{entityName}/start_date={startTime}/end_date={endTime}/batch_id={batchId}/
  ├── page-0001.json
  ├── page-0002.json
  ├── page-XXXX.json
  └── manifest.json
  ```

## Data Retrieval Flow

### 1. Request Validation & Initialization
```javascript
Request Body Requirements:
├── adf_pipeline_run_id (required)     // Azure Data Factory pipeline run ID
├── start_time (required)              // Format: YYYY-MM-DD
├── end_time (required)                // Format: YYYY-MM-DD
├── source_name (optional)             // Default: "controlio"
├── pipeline_type (optional)           // Default: "raw"
├── entity_name (optional)             // Default: "timeline"
├── trigger_time_utc (optional)        // Default: current UTC time
├── window_start_utc (optional)        // ISO format timestamp
├── window_end_utc (optional)          // ISO format timestamp
└── resume_batch_id (optional)         // For resuming interrupted batches
```

### 2. Batch Log Management
Trước khi lấy dữ liệu, Bronze function thực hiện các bước sau:

#### a) Query Batch (Check Idempotency)
```javascript
const existingBatch = await this.batchLogClient.getBatch(batchLogPartitionKey, batchId);

if (existingBatch?.bronze_status === "Succeeded") {
    // Skip: Batch đã hoàn thành, return cached result
    return { skipped: true, ... };
}
```

**Purpose**: Kiểm tra batch đã tồn tại và đã hoàn thành chưa
- **Partition Key**: `{sourceName}|{pipelineType}|{entityName}|{batchMonth}`
- **Row Key**: `batch_id` (unique identifier)

#### b) Create or Update Batch

**Nếu batch không tồn tại** → Tạo mới với trạng thái `Created`:
- `bronze_status: NotStarted`
- `batch_status: Created`
- `current_stage: Bronze`

**Nếu batch tồn tại nhưng chưa hoàn thành** → Update:
- `current_adf_pipeline_run_id`: Update với run ID mới
- `run_mode`: Tự động detect (Resume/Manual/Scheduled)
- `rerun_count`: Tăng nếu run ID khác

#### c) Batch Status Lifecycle
```
NotStarted → Created → Running → Succeeded (or Failed)
```

**Mỗi stage được track riêng**:
- `bronze_status`: Status của Bronze processing
- `silver_status`: Status của Silver processing
- `gold_status`: Status của Gold processing
- `current_stage`: Stage hiện tại đang chạy

### 3. Checkpoint Management
- Lấy checkpoint cursor từ Table Storage: `Checkpoint|{sourceName}|{pipelineType}`
- Cursor được sử dụng để continue pagination từ điểm dừng trước
- Cho phép resume batch processing nếu bị interrupt

### 4. Pagination Loop

Bronze function thực hiện pagination lặp lại cho đến khi lấy hết dữ liệu:

#### API Query Parameters

```javascript
// Fixed parameters
start_time              // Ngày bắt đầu (YYYY-MM-DD)
end_time                // Ngày kết thúc (YYYY-MM-DD)
limit                   // Số records per page (Default: 10,000)
sort_direction          // Always: "asc"
prev                    // Pagination cursor (null cho page đầu)

// Optional filters
activities              // Comma-separated IDs
users                   // Comma-separated user IDs
departments             // Comma-separated department IDs
activity_type           // Integer type code
string_to_match         // Text search filter
```

#### Pagination Logic

```
1. Start: currentPrev = null (hoặc từ checkpoint/resume)
2. Loop:
   ├─ API Call: GET /api/v1/statistics/timeline?start_time=...&end_time=...&prev={currentPrev}
   ├─ Wait for retry with exponential backoff (if failed)
   │  ├─ Max attempts: 3 (default)
   │  ├─ Timeout: 60 seconds per request (default)
   │  └─ Max total requests per batch: 100 pages (default)
   ├─ Receive: Array of rows
   ├─ Extract: Pagination cursor từ last row
   ├─ Save: Page JSON file to Blob Storage
   ├─ Update: Batch log (page count, row count, cursor)
   └─ Continue if: rows.length == limit (có thêm dữ liệu)
3. End: paginationComplete = true
```

### 5. Page File Content Structure

Mỗi page file (`page-0001.json`, etc.) chứa:

```json
{
  "batch_id": "raw_timeline_20231215T120000Z_abc12345",
  "page_number": 1,
  "input_prev": null,                          // Cursor đầu vào
  "output_prev": "eyJpZCI6IDEyMzQ1LCAidHM...", // Cursor cuối ra
  "next_request_prev": "eyJpZCI6IDEyMzQ1LCAid...", // Cursor cho page tiếp theo
  "row_count": 10000,
  "fetched_at_utc": "2023-12-15T12:00:00.000Z",
  "request_parameters": {
    "start_time": "2023-12-14",
    "end_time": "2023-12-15",
    "limit": 10000,
    "sort_direction": "asc",
    "prev": null,
    "activities": "123,456,789",
    "users": "user1,user2",
    "departments": "dept1",
    "activity_type": 1,
    "string_to_match": "search_keyword"
  },
  "response": {                                 // Raw Controlio API response
    "data": [
      // Raw records từ Controlio
    ]
  }
}
```

### 6. Manifest File

Sau khi hoàn thành toàn bộ pagination, Bronze tạo `manifest.json`:

```json
{
  "batch_id": "raw_timeline_20231215T120000Z_abc12345",
  "source_name": "controlio",
  "entity_name": "timeline",
  "pipeline_type": "raw",
  "adf_pipeline_run_id": "abc12345-1234-1234-1234-123456789abc",
  "start_time": "2023-12-14",
  "end_time": "2023-12-15",
  "start_date": "2023-12-14",
  "end_date": "2023-12-15",
  "window_start_utc": "2023-12-14T00:00:00.000Z",
  "window_end_utc": "2023-12-15T23:59:59.999Z",
  "input_cursor_prev": null,                   // Checkpoint cursor hoặc null
  "output_cursor_prev": "eyJpZCI6IDE3MTI5...", // Final cursor
  "page_count": 15,                            // Tổng số pages lấy được
  "raw_row_count": 142500,                     // Tổng số records
  "api_call_count": 16,                        // Tổng API calls (có retry)
  "bronze_path": "bronze/controlio/timeline/start_date=2023-12-14/end_date=2023-12-15/batch_id=raw_timeline_20231215T120000Z_abc12345",
  "status": "Succeeded",
  "completed_at_utc": "2023-12-15T12:15:30.000Z"
}
```

## Configuration Parameters

### Environment Variables

| Variable | Type | Default | Max | Description |
|----------|------|---------|-----|-------------|
| `CONTROLIO_BASE_URL` | string | - | - | Base URL của Controlio API |
| `CONTROLIO_TOKEN_SECRET_NAME` | string | - | - | Key Vault secret name cho Bearer token |
| `CONTROLIO_PAGE_LIMIT` | integer | 10,000 | 10,000 | Records per page |
| `CONTROLIO_MAX_API_ATTEMPTS` | integer | 3 | 10 | Max retry attempts per API call |
| `CONTROLIO_REQUEST_TIMEOUT_MS` | integer | 60,000 | 220,000 | Request timeout in milliseconds |
| `MAX_PAGES_PER_BATCH` | integer | 100 | 100,000 | Max pages per batch |
| `DATA_STORAGE_ACCOUNT` | string | - | - | Azure Storage account name |
| `BRONZE_CONTAINER` | string | - | - | Blob container for Bronze data |
| `BATCH_LOG_TABLE` | string | - | - | Table Storage for batch tracking |
| `CHECKPOINT_TABLE` | string | - | - | Table Storage for pagination checkpoints |
| `KEY_VAULT_URL` | string | - | - | Azure Key Vault URL |

### Default Values Applied
- `source_name`: "controlio" (if not provided)
- `pipeline_type`: "raw" (if not provided)
- `entity_name`: "timeline" (if not provided)
- `trigger_type`: "Manual" (if not provided)
- `adf_pipeline_name`: "pl_controlio_timeline" (if not provided)

## Time Range

### Time Parameters

1. **start_time & end_time** (Required)
   - Format: `YYYY-MM-DD`
   - Định nghĩa date range lấy dữ liệu từ Controlio
   - Ví dụ: `start_time=2023-12-14`, `end_time=2023-12-15`

2. **window_start_utc & window_end_utc** (Optional)
   - Format: ISO 8601 timestamp (e.g., `2023-12-14T00:00:00Z`)
   - Nếu `start_time`/`end_time` không cung cấp, lấy từ window
   - Dùng để báo cáo processing window

3. **trigger_time_utc** (Optional)
   - Default: Current UTC time
   - Timestamp khi pipeline triggered
   - Dùng để generate batch ID

### Example Request

```json
{
  "adf_pipeline_run_id": "abc12345-1234-1234-1234-123456789abc",
  "start_time": "2023-12-01",
  "end_time": "2023-12-31",
  "window_start_utc": "2023-12-01T00:00:00Z",
  "window_end_utc": "2023-12-31T23:59:59Z",
  "source_name": "controlio",
  "pipeline_type": "raw",
  "entity_name": "timeline",
  "activities": "123,456,789",
  "users": "john.doe,jane.smith",
  "departments": "IT,HR",
  "activity_type": 1,
  "string_to_match": "urgent"
}
```

## Error Handling

### Idempotency
- Nếu batch đã `Succeeded`, Bronze sẽ skip và return cached result
- Không re-fetch data nếu batch đã hoàn thành

### Retry Logic
- Exponential backoff cho API failures
- Max 3 attempts per request (configurable)
- 60s timeout per request (configurable)

### Max Pages Limit
- Nếu vượt 100 pages, throw error (prevent infinite loops)
- Có thể resume batch sau khi fix issue

## Performance Metrics

Batch log track các metrics sau:
- `page_count`: Số pages lấy được
- `raw_row_count`: Tổng records
- `api_call_count`: Tổng API calls (include retries)
- `bronze_started_at`: Start timestamp
- `bronze_finished_at`: End timestamp

## Resume & Checkpoint

### Resume Batch
- Gửi `resume_batch_id` trong request body
- Function sẽ lấy state từ batch log
- Continue từ `last_persisted_cursor_prev`

### Checkpoint
- Lưu committed cursor cho mỗi (sourceName, pipelineType)
- Update sau khi Silver successfully process page
- Cho phép next batch start từ checkpoint

## Summary

| Aspect | Details |
|--------|---------|
| **Data Source** | Controlio API (`/api/v1/statistics/timeline`) |
| **Retrieval Method** | Paginated REST API calls with Bearer token |
| **Data Filters** | Activities, Users, Departments, Activity Type, String search |
| **Time Range** | start_time to end_time (date range, YYYY-MM-DD) |
| **Storage** | Azure Blob Storage (JSON pages + manifest) |
| **Retry Strategy** | Exponential backoff, max 3 attempts, 60s timeout |
| **Pagination** | Cursor-based with max 100 pages per batch |
| **Tracking** | Batch log in Table Storage for state management |
| **Features** | Idempotent, resumable, checkpoint-based continuation |
