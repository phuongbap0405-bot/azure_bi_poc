# Error Handling Strategy

## 1. Retry and Idempotency Principles

### Idempotency

All retry mechanisms at the Azure Function level and ADF level must be safe and must not cause duplicate data. This is achieved by:

* Reusing the same `batch_id` across all retry attempts.
* Always using **Overwrite** mode when writing files to Bronze and Silver.
* Always using **MERGE** (Insert / Update / Skip) when writing to Gold.

---

## 2. API Failure

### Cause

* HTTP 429 — Rate limit exceeded on Controlio API
* Controlio unavailable — HTTP 500, 502, 503
* Network interruption or timeout

### Strategy

* **Function-level:** Perform internal retry with exponential backoff inside the Azure Function.
* **ADF-level:** Fail the pipeline activity and allow ADF to perform activity-level retry.
* Write `error_category = Transient` to the BatchLog table.
* Fail the pipeline permanently if ADF retry limit is exhausted (recommended: 3 retries).

---

## 3. Authentication Failure

### Cause

* Invalid token — HTTP 401
* Expired credentials
* Misconfigured secret in Key Vault

### Strategy

* **Important:** This is a **Permanent Error**. The system already retrieves the latest token before calling the API. A 401 means the configuration is incorrect.
* **DO NOT RETRY AUTOMATICALLY.** Retrying risks triggering an account lockout on the Controlio API.
* Stop all processing immediately.
* Fail the pipeline and raise an alert for the support team to inspect Key Vault.

---

## 4. Data Validation Failure (Silver Layer)

### Cause

* Missing required fields
* Schema mismatch
* Invalid JSON payload from Bronze

### Strategy

* Preserve the raw Bronze data for investigation.
* Mark the Silver stage as Failed and do not proceed to Gold.
* Write `error_category = DataQuality` and `failed_stage = Silver` to BatchLog.
* Notify the support team to review field mapping or data format.

---

## 5. Storage Failure

### Cause

* ADLS temporarily unavailable
* Permission or RBAC misconfiguration

### Strategy

* Retry the write operation if the error is transient.
* If retry fails, stop the pipeline.
* **Preserve processing state:** Write `error_category` and `failed_stage` to the **BatchLog** table so that a manual resume run knows exactly where processing was interrupted.

---

## 6. Gold Layer and Checkpoint Concurrency Failure

### Cause

* Delta transaction failure or merge conflict
* Checkpoint concurrency conflict — HTTP 412 Precondition Failed due to ETag mismatch

### Strategy

* **Delta failure:** Roll back the current transaction. Gold layer remains at the previous version. Allow ADF to retry the entire Gold activity.
* **Checkpoint concurrency conflict:**
  1. Re-read the latest Checkpoint record.
  2. If Checkpoint already points to the current `batch_id` → treat as idempotent success and return success to ADF.
  3. If Checkpoint points to a different `batch_id` → set `batch_status = ManualReview`, write `error_category = Concurrency` to BatchLog, and stop the pipeline.

---

## 7. Reporting Impact

### Strategy

Power BI reports continue to use data from the most recent successful Gold version until a new pipeline cycle completes successfully.
