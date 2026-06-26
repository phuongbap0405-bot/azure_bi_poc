# Silver Function - Data Processing

## Mục đích
Silver function xử lý dữ liệu từ Bronze layer và chuyển thành dữ liệu clean chuẩn hoá, lưu dưới dạng Parquet.
Nó thực hiện:
- đọc Bronze manifest và pages
- normalize từng record
- loại bỏ bản sao (deduplicate)
- phân loại record hợp lệ/không hợp lệ
- ghi output clean vào Parquet
- ghi rejected record vào JSONL
- cập nhật BatchLog và manifest của Silver

## Input request
Silver function nhận POST request JSON với:
- `batch_id` (required)
- `bronze_path` (required)

Ví dụ:
```json
{
  "batch_id": "raw_timeline_20231215T120000Z_abc12345",
  "bronze_path": "controlio/timeline/start_date=2023-12-14/end_date=2023-12-15/batch_id=raw_timeline_20231215T120000Z_abc12345"
}
```

## Bước xử lý chính

### 1. Validate request
- `batch_id` phải tồn tại và được sanitize
- `bronze_path` phải là path hợp lệ trong Bronze container

### 2. Đọc Bronze manifest
- đọc file: `${BRONZE_CONTAINER}/${bronzePrefix}/manifest.json`
- kiểm tra `batch_id` trùng với request
- kiểm tra `status === "Succeeded"`

Nếu Bronze manifest không hợp lệ, Silver sẽ lỗi dừng ngay.

### 3. Trường hợp Bronze không có record
Nếu `bronze_manifest.raw_row_count === 0` thì:
- tạo Silver manifest với `status: "Skipped"`
- không tạo file Parquet
- cập nhật BatchLog:
  - `silver_status: "Skipped"`
  - `current_stage: "Gold"`
- trả về success + skipped

### 4. Nếu Bronze có record
Silver sẽ tiếp tục:
- lấy `page_count` từ Bronze manifest
- duyệt từng page từ `page-0001.json` đến `page-{pageCount}.json`
- mỗi page chứa `response.data` là array record raw

### 5. Normalize + deduplicate + phân loại
Với mỗi `rawRecord`:
- tạo `metadata` chứa `batchId`, `pageNumber`, `sourceName`, `entityName`, `ingestedAtUtc`
- gọi `normalizeTimelineRecord(rawRecord, metadata)`
- normalize gồm:
  - parse `user_id`, `computer_id`, `activity_id`, `activity_type`
  - chuẩn hoá `start_time`, `end_time` thành UTC ISO string
  - tính `duration_seconds`
  - trích domain từ URL
  - tạo `activity_date`
  - thêm `unique_key` từ hashing record raw
- nếu normalize thành công:
  - nếu duplicate thì bỏ qua
  - nếu không duplicate thì push vào `cleanBuffer`
- nếu normalize lỗi:
  - tạo rejected record và push vào `rejectedBuffer`

### 6. Deduplicate
Silver dùng in-memory `Deduplicator` để giữ một `Set` của `unique_key` trong một lần chạy batch.
- duplicate count tăng lên khi `unique_key` trùng
- duplicate record không được ghi vào Parquet

### 7. Viết file output
Silver tạo:
- clean output files: `part-0001.parquet`, `part-0002.parquet`, ...
- rejected output files: `rejected/part-0001.jsonl`, ...

Quy tắc flush:
- `cleanBuffer` được flush khi đủ `silverRecordsPerFile` record
- `rejectedBuffer` cũng flush theo cùng threshold
- `silverRecordsPerFile` lấy từ config env `SILVER_RECORDS_PER_FILE`

### 8. Schema Parquet
Silver dùng `parquetjs-lite` và schema:
- `unique_key`
- `batch_id`
- `source_page_number`
- `source_name`
- `entity_name`
- `activity`, `activity_id`, `activity_type`, `activity_name`, `caption`
- `user_id`, `computer_id`
- `start_time_utc`, `end_time_utc`, `activity_date`, `duration_seconds`
- `is_website`, `url_domain`, `ingested_at_utc`

Tất cả timestamp được serialize thành ISO-8601 string.

### 9. Silver manifest
Sau khi hoàn thành, Silver tạo manifest trong Silver container:
- `batch_id`
- `source_name`, `entity_name`, `pipeline_type`
- `start_time`, `end_time`
- `bronze_path`
- `silver_path`
- `source_page_count`, `source_row_count`
- `clean_row_count`, `rejected_row_count`, `duplicate_row_count`
- `clean_file_count`, `rejected_file_count`
- `output_files`
- `output_format: "parquet"`
- `status: "Succeeded"`
- `completed_at_utc`

### 10. Cập nhật BatchLog
Nếu Silver thành công, cập nhật bảng BatchLog với:
- `silver_path`
- `silver_status: "Succeeded"`
- `current_stage: "Gold"`
- counts: clean, rejected, duplicate
- `silver_file_count`
- `silver_finished_at`

Nếu Silver đã chạy thành công trước đó, function sẽ skip idempotently.

## Tóm tắt xử lý

| Chức năng | Mô tả |
|---|---|
| Đọc Bronze manifest | Xác nhận batch có sẵn và Bronze đã thành công |
| Read Bronze pages | Lấy dữ liệu raw từ Bronze JSON pages |
| Normalize | Chuẩn hoá field, parse timestamp, build metadata |
| Deduplicate | Loại bỏ duplicate cùng batch |
| Rejected | Ghi các record lỗi vào JSONL |
| Parquet output | Ghi clean records ra Parquet |
| BatchLog update | Ghi kết quả Silver và chuyển stage sang Gold |

## Phạm vi xử lý
Silver chỉ xử lý dữ liệu từ Bronze layer đã có manifest `Succeeded` và không thực hiện gọi Controlio API trực tiếp.

## Kết luận
Silver function đang thực hiện bước "clean and shape" dữ liệu Bronze:
- đọc lại dữ liệu raw từ Bronze
- chuẩn hoá cấu trúc record
- loại bỏ duplicate
- ghi ra Parquet cho downstream
- ghi rejected record nếu không normalize được
- cập nhật batch trạng thái để tiếp tục sang Gold
