# Microsoft Fabric Modern DataOps Retail Platform

An end-to-end **Microsoft Fabric data engineering portfolio project**
that implements a production-style Modern Data Warehouse and evolves it
into a **Modern DataOps platform**.

The solution demonstrates ingestion, medallion architecture, PySpark
transformations, Delta Lake, incremental loading, metadata-driven
processing, data-quality quarantine, idempotency, pipeline auditing,
dimensional modelling, automated validation, Git integration, and Azure
DevOps CI/CD.

> **Project goal:** Build a realistic Microsoft Fabric solution that
> goes beyond basic ETL by showing how a data platform can be
> incrementally loaded, monitored, tested, governed, rerun safely, and
> promoted through DEV, TEST, and PROD environments.

## Architecture

``` text
Source CSV Files
       |
       v
Fabric Data Factory
       |
       v
OneLake / Fabric Lakehouse
       |
       v
+-------------------+
|      BRONZE       |
| Raw / batch data  |
+---------+---------+
          |
          v
   PySpark Notebooks
   - schema enforcement
   - validation
   - deduplication
   - incremental filtering
   - Delta MERGE
          |
     +----+----+
     |         |
     v         v
  SILVER    QUARANTINE
 Delta Tables  Invalid Rows
     |
     v
Fabric Warehouse
     |
     v
DimCustomer | DimProduct | DimStore | DimDate
FactSales   | FactInventory
     |
     v
Analytics-ready dimensional model
```

The project uses a medallion-style design:

-   **Bronze** --- raw source data retained in OneLake using batch-based
    folder structures.
-   **Silver** --- cleansed and validated Delta tables produced with
    PySpark, including incremental `MERGE` processing.
-   **Gold** --- Fabric Warehouse dimensional model containing
    dimensions and fact tables for analytics.

See [Architecture](architecture/architecture.md) for more detail.

## Technology Stack

  Area                     Technology
  ------------------------ ---------------------------------------------
  Analytics platform       Microsoft Fabric
  Storage                  OneLake
  Data engineering         Fabric Lakehouse
  Orchestration            Fabric Data Factory pipelines
  Transformations          PySpark
  Table format             Delta Lake
  Warehouse                Fabric Warehouse
  SQL                      T-SQL
  Architecture             Medallion Architecture
  Incremental processing   Watermarks + Delta `MERGE`
  Data quality             Validation + quarantine tables
  Operational control      Metadata, watermarks, pipeline audit tables
  Testing                  PySpark validation + Python/Pytest
  Source control           Git / GitHub
  CI/CD                    Azure DevOps + Fabric deployment automation

## Phase 1 --- End-to-End Data Platform

Phase 1 established the core end-to-end Fabric data flow:

``` text
CSV Source
    -> Fabric Pipeline
    -> Bronze
    -> PySpark
    -> Silver Delta
    -> Fabric Warehouse
```

The objective was to prove the complete architecture before introducing
production-style DataOps controls.

Key capabilities implemented:

-   Source CSV ingestion using Fabric Data Factory pipelines.
-   Raw data landing in the Bronze layer in OneLake.
-   PySpark transformations for cleansing and validation.
-   Silver Delta tables for Customers, Products, Stores, Orders, Order
    Items, and Inventory.
-   Data-quality quarantine for invalid Orders and Order Items.
-   Fabric Warehouse dimensional model with dimensions and facts.

See [Phase 1](docs/Phase-1.md).

## Phase 2 --- Modern DataOps

Phase 2 upgraded the solution from a development-style overwrite process
into an incremental, auditable, rerunnable DataOps framework.

Capabilities added:

-   Incremental ingestion.
-   Source timestamp watermarks.
-   Delta Lake `MERGE` upserts.
-   Stable business keys and composite keys.
-   Metadata-driven entity configuration.
-   Batch IDs and source lineage metadata.
-   Pipeline execution auditing.
-   Idempotent processing.
-   Data-quality quarantine.
-   Automated validation.
-   Incremental Fabric Warehouse `MERGE` loading.
-   DEV / TEST / PROD environment configuration.
-   Git integration.
-   Azure DevOps CI/CD.

See [Phase 2](docs/Phase-2.md).

## DataOps Monitoring and Control Framework

The Lakehouse contains three Delta control tables that make the
incremental framework observable and configurable.

### `ctl_entity_config`

Defines **what should be processed and how**. It stores entity-level
metadata such as:

-   entity name
-   Silver target table
-   business key
-   watermark column
-   whether loading is enabled
-   processing order

This reduces hard-coded processing behaviour and provides a central
configuration layer for entities such as Customers, Products, Stores,
Orders, Order Items, and Inventory.

### `ctl_entity_watermark`

Tracks **where each successful incremental load finished**.

The pipeline compares incoming `_source_updated_at` values with the
stored watermark and processes only records newer than the previous
successful position. After successful processing, the watermark, batch
ID, and success timestamp are updated.

### `ctl_pipeline_run_audit`

Records **what happened during each pipeline execution**, including:

-   unique run ID
-   pipeline name
-   batch ID
-   environment
-   start and end time
-   `RUNNING`, `SUCCEEDED`, or `FAILED` status
-   entities processed
-   rows inserted
-   rows updated
-   rows rejected
-   error information

This provides an operational history for monitoring, troubleshooting,
and validating pipeline executions.

See [DataOps Monitoring](docs/DataOps-Monitoring.md).

## Incremental Loading and Idempotency

A key Phase 2 objective was to make the pipeline **idempotent**:
rerunning the same batch must not create duplicate business records.

The solution combines:

1.  `SourceUpdatedAt` / `_source_updated_at` watermarks.
2.  Stable business keys.
3.  Delta Lake `MERGE`.
4.  Fabric Warehouse `MERGE`.
5.  Batch-aware quarantine logic.

Conceptually:

``` text
Incoming Record
      |
      v
Newer than watermark?
   |             |
  No            Yes
   |             |
 Skip       Validate Record
                 |
          +------+------+
          |             |
        Valid         Invalid
          |             |
          v             v
     Delta MERGE    Quarantine
          |
          v
    Update Watermark
```

See [Idempotency](docs/Idempotency.md).

## Data Quality and Quarantine

Invalid records are not silently discarded.

Records that fail validation are written to quarantine tables with a
`validation_error` describing why they were rejected.

Example test record:

``` text
OrderItemID: OI2000999
Quantity: 0
validation_error: Quantity must be > 0
```

The invalid Order Item is prevented from entering `silver_order_items`
while remaining available in quarantine for investigation.

See [Data Quality](docs/Data-Quality.md).

## Fabric Warehouse Model

The Gold layer is implemented in **Fabric Warehouse** using a
Kimball-style dimensional model.

### Dimensions

-   `DimCustomer`
-   `DimProduct`
-   `DimStore`
-   `DimDate`

### Facts

-   `FactSales`
-   `FactInventory`

Phase 2 replaces full rebuild behaviour with T-SQL `MERGE` logic so
existing dimensional records can be updated and new records inserted
incrementally while retaining stable warehouse keys.

## Automated Validation

Validation is included as part of the processing lifecycle rather than
being treated as a separate manual activity.

Examples include checks that:

-   expected incremental Customers, Products, Stores, and Orders were
    loaded;
-   invalid records did not enter valid Silver tables;
-   rejected records were written to quarantine;
-   rerunning the same batch does not increase business-row counts
    unexpectedly.

The repository also contains Python/Pytest transformation tests under
[`tests/`](tests/).

## CI/CD and Environment Promotion

The project includes a DataOps deployment pattern for promoting Fabric
changes through controlled environments.

``` text
Developer / Feature Branch
          |
          v
      Pull Request
          |
          v
     Automated Tests
          |
          v
         DEV
          |
          v
         TEST
          |
          v
   Validation / Approval
          |
          v
         PROD
```

Azure DevOps pipeline definitions are stored under
[`azure-devops/`](azure-devops/), while deployment helper logic is
stored under [`.deploy/`](.deploy/).

The deployment design separates orchestration from deployment logic:

``` text
Deploy-To-Fabric.yml
        |
        v
deploy-to-fabric.py
        |
        v
Fabric deployment automation
        |
   +----+----+
   |         |
   v         v
 TEST       PROD
```

Secrets such as client secrets, tokens, and credentials are
intentionally excluded from source control.

## Repository Structure

``` text
microsoft-fabric-retail-dataops-platform/
|
|-- README.md
|
|-- architecture/
|   |-- architecture.md
|   |-- architecture-overview.png
|   |-- medallion-architecture.png
|   `-- cicd-flow.png
|
|-- docs/
|   |-- Phase-1.md
|   |-- Phase-2.md
|   |-- DataOps-Monitoring.md
|   |-- Data-Quality.md
|   |-- Idempotency.md
|   `-- screenshots/
|
|-- data/
|   |-- initial-load/
|   `-- incremental/
|
|-- notebooks/
|   |-- NB_00_Setup_Control_Framework.py
|   |-- NB_01_Bronze_To_Silver.py
|   |-- NB_02_Incremental_Bronze_To_Silver.py
|   `-- NB_03_Validate_Incremental_Load.py
|
|-- sql/
|   |-- 01_BUILD_RETAIL_WAREHOUSE.sql
|   |-- 03_CREATE_INCREMENTAL_WAREHOUSE_LOAD.sql
|   `-- 04_VALIDATE_PHASE2_WAREHOUSE.sql
|
|-- tests/
|   |-- transformations.py
|   `-- test_transformations.py
|
|-- config/
|   |-- dev.json
|   |-- test.json
|   `-- prod.json
|
|-- azure-devops/
|   |-- azure-pipelines-ci.yml
|   `-- Deploy-To-Fabric.yml
|
|-- .deploy/
|   |-- deploy-to-fabric.py
|   `-- parameter.yml
|
`-- fabric/
    `-- Fabric Git-integrated workspace item definitions
```

## Proof of Concept

The repository documentation can include screenshots from the
implemented Fabric environment to demonstrate that the code was executed
successfully.

Recommended evidence under `docs/screenshots/`:

  -----------------------------------------------------------------------
  Screenshot                          Demonstrates
  ----------------------------------- -----------------------------------
  Fabric DEV workspace                Implemented Fabric items and
                                      workspace structure

  Phase 1 ingestion pipeline          Initial end-to-end ingestion
                                      orchestration

  Phase 2 DataOps pipeline            Incremental orchestration and
                                      downstream processing

  Bronze incremental folders          Batch-based OneLake ingestion
                                      structure

  Silver and control tables           Delta tables and operational
                                      framework

  Watermark table                     Incremental state management

  Pipeline audit table                Execution monitoring and row
                                      metrics

  Quarantine output                   Data-quality rejection handling

  Warehouse model                     Dimensions and fact tables

  Successful Phase 2 run              End-to-end execution evidence

  Git integration                     Fabric workspace source-control
                                      integration

  Azure DevOps pipeline               CI/CD execution and environment
                                      promotion
  -----------------------------------------------------------------------

## Key Engineering Outcomes

This project demonstrates the ability to design and implement more than
a simple data movement pipeline. It incorporates engineering patterns
required to operate a maintainable data platform, including:

-   end-to-end Microsoft Fabric data engineering;
-   medallion architecture and OneLake organisation;
-   PySpark and Delta Lake transformation patterns;
-   metadata-driven incremental processing;
-   watermark-based change processing;
-   business-key-based `MERGE` operations;
-   idempotent pipeline reruns;
-   data-quality validation and quarantine;
-   pipeline auditing and operational observability;
-   dimensional warehouse design;
-   automated testing;
-   source control and CI/CD practices;
-   DEV, TEST, and PROD deployment design.

## Project Status

-   **Phase 1:** Completed --- end-to-end Fabric ingestion,
    transformation, and warehouse flow.
-   **Phase 2:** Completed --- incremental loading, DataOps controls,
    validation, auditing, idempotency, and CI/CD design.

## Notes

This is a portfolio / proof-of-concept project built to demonstrate
practical Microsoft Fabric data engineering and Modern DataOps patterns.
Sample data is used for demonstration purposes, and secrets or
production credentials are not stored in the repository.
