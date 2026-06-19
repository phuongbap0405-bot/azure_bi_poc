# Technology Stack

| Layer                     | Technology                   | Purpose                                             | Reason for Selection                                                                                                   | Alternative Considered                          |
| ------------------------- | ---------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Orchestration             | Azure Data Factory           | Schedule and coordinate processing workflows        | Native Azure orchestration service, visual monitoring, retry handling, dependency management, low operational overhead | Azure Logic Apps, Azure Functions Timer Trigger |
| Compute                   | Azure Function      | Call Controlio APIs and perform data transformation | Serverless, cost-effective, flexible coding model, suitable for custom API integration and Delta processing            | ADF Mapping Data Flow, Azure Databricks         |
| Source System             | Controlio REST API           | Provide activity monitoring data                    | System of record for productivity and activity tracking                                                                | N/A                                             |
| Storage                   | Azure Data Lake Storage Gen2 | Centralized storage for all data layers             | Low-cost scalable storage, supports JSON, Parquet, Delta, lifecycle management, integrates with Power BI and Fabric    | Azure SQL Database, Synapse Dedicated SQL Pool  |
| Bronze Layer              | JSON                         | Store raw source data                               | Preserve original payload for auditability and reprocessing                                                            | CSV                                             |
| Silver Layer              | Parquet                      | Store cleaned and standardized data                 | Columnar format, compressed storage, optimized analytical reads                                                        | CSV, JSON                                       |
| Gold Layer                | Delta Lake                   | Reporting-ready datasets                            | Supports ACID transactions, MERGE, UPSERT, versioning, schema evolution                                                | Parquet only                                    |
| Authentication            | Managed Identity             | Secure service-to-service authentication            | Eliminates secret management in code and supports Azure RBAC                                                           | Storage Access Keys                             |
| Secret Management         | Azure Key Vault              | Store API credentials and secrets                   | Centralized secret management with rotation support                                                                    | Application Settings                            |                              |

---



## Design Principles

The technology stack is selected based on the following principles:

* ADLS is the single source of truth.
* Minimize infrastructure and operational costs.
* Prefer serverless services when possible.
* Separate storage from compute.
* Support future integration with Microsoft Fabric.
* Support incremental processing and long-term historical storage.
* Minimize vendor lock-in at the reporting layer.

---

# Tech Stack Decisions

## Azure Data Factory

### Alternatives Considered

* Azure Function Timer Trigger
* Azure Logic Apps
* Azure Data Factory

### Decision

Use Azure Data Factory as the orchestration layer.

### Justification

The solution requires:

* Scheduled execution every 10 minutes
* Dependency management between Bronze, Silver, and Gold processing
* Centralized monitoring
* Retry and failure handling
* Operational visibility

Azure Function Timer Trigger can schedule execution but requires custom orchestration logic and monitoring.

Azure Logic Apps is suitable for workflow automation but provides limited value for a data engineering workload.

Azure Data Factory provides:

* Native scheduling
* Activity dependency management
* Built-in retry policies
* Pipeline monitoring
* Operational dashboard

Therefore, Azure Data Factory provides the best balance between maintainability and operational control.

---

## Azure Function

### Alternatives Considered

* ADF Copy Activity
* ADF Mapping Data Flow
* Azure Databricks
* Azure Function

### Decision

Use Azure Function for API integration and transformation logic.

### Justification

Controlio APIs require custom processing logic, including:

* Incremental loading using prev cursor
* Pagination handling
* Retry handling
* Data validation
* Deduplication
* Business rule processing

ADF Copy Activity is optimized for data movement rather than custom API workflows.

ADF Mapping Data Flow requires Spark clusters and introduces significantly higher compute costs for a workload that runs every 10 minutes.

Azure Databricks provides powerful distributed processing but introduces unnecessary operational complexity and cost for the expected workload size.

Expected workload:

* Approximately 100 monitored employees
* Approximately 200 GB raw data per year
* Batch processing every 10 minutes

Azure Function provides sufficient processing capability while maintaining low operational cost.

---

## Azure Data Lake Storage Gen2

### Alternatives Considered

* Azure SQL Database
* Azure Synapse Dedicated SQL Pool
* Microsoft Fabric Lakehouse
* Azure Data Lake Storage Gen2

### Decision

Use ADLS Gen2 as the single source of truth.

### Justification

The solution requires storage for:

* Raw API payloads
* Clean analytical datasets
* Reporting datasets
* Long-term historical retention

ADLS provides several advantages:

### Separation of Storage and Compute

Storage remains independent from:

* Reporting engines
* Transformation engines
* Analytics platforms

This allows Power BI, Fabric, or future analytics tools to consume the same data without duplication.

### Support for Multiple Data Formats

The architecture requires:

* Bronze JSON
* Silver Parquet
* Gold Delta

ADLS natively supports all required formats.

### Cost Efficiency

Controlio data volume is expected to grow continuously over time.

Keeping large historical datasets inside database platforms would increase storage and compute costs unnecessarily.

ADLS provides lower storage costs and lifecycle management capabilities.

---

## Parquet for Silver Layer

### Alternatives Considered

* JSON
* CSV
* Parquet

### Decision

Use Parquet for the Silver layer.

### Justification

The Silver layer is intended for analytical processing rather than operational storage.

Parquet provides:

* Columnar storage
* Compression
* Faster analytical reads
* Reduced storage consumption

Compared to JSON, Parquet significantly reduces storage requirements and improves query performance.

---

## Delta Lake for Gold Layer

### Alternatives Considered

* Parquet
* Delta Lake

### Decision

Use Delta Lake for the Gold layer.

### Justification

The reporting layer requires:

* Incremental updates
* Upserts
* Deduplication
* Transactional consistency

Delta Lake provides:

* ACID transactions
* MERGE support
* UPSERT support
* Schema evolution
* Version history

Parquet alone does not provide transactional update capabilities.

---

## Managed Identity for Authentication

### Alternatives Considered

* Storage Access Keys
* Shared Access Signatures (SAS)
* Managed Identity

### Decision

Use Managed Identity.

### Justification

Managed Identity eliminates the need to store credentials in application code.

Benefits include:

* Secretless authentication
* Centralized access control through RBAC
* Reduced operational risk
* Easier credential rotation

This aligns with Azure security best practices.


