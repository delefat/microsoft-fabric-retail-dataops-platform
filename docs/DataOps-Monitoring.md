# DataOps Monitoring and Control Framework

To make the data platform operationally reliable and support
production-style DataOps practices, the solution includes a
metadata-driven monitoring and control framework.

Three Delta control tables are maintained within the Microsoft Fabric
Lakehouse:

-   `ctl_entity_config`
-   `ctl_entity_watermark`
-   `ctl_pipeline_run_audit`

Together, these tables provide configuration management,
incremental-load state management, pipeline observability, auditability,
and troubleshooting capabilities.

## 1. Entity Configuration --- `ctl_entity_config`

The `ctl_entity_config` table defines how each business entity should be
processed by the data pipeline.

Rather than hard-coding all processing behaviour into individual
notebooks, metadata is stored centrally for entities such as Customers,
Products, Stores, Orders, Order Items, and Inventory.

  -----------------------------------------------------------------------
  Column                              Purpose
  ----------------------------------- -----------------------------------
  `entity_name`                       Identifies the source entity being
                                      processed

  `target_table`                      Defines the Silver Delta table into
                                      which the entity is loaded

  `business_key`                      Defines the key used to uniquely
                                      identify records during MERGE
                                      operations

  `watermark_column`                  Identifies the timestamp column
                                      used for incremental processing

  `load_enabled`                      Determines whether the entity
                                      should participate in processing

  `load_order`                        Defines the logical processing
                                      order of entities
  -----------------------------------------------------------------------

Example processing metadata:

``` text
customers
    ↓
Business Key: CustomerID
    ↓
Target: silver_customers
    ↓
Watermark: _source_updated_at
```

The `load_order` allows dependencies between datasets to be represented.
Customers, Products, and Stores should be available before Orders and
Order Items are processed.

This metadata-driven approach makes the framework easier to maintain and
extend because new entities and processing rules can be managed
centrally instead of duplicating configuration throughout the code.

## 2. Incremental Load Watermark --- `ctl_entity_watermark`

The `ctl_entity_watermark` table maintains the processing position of
each entity.

A watermark records the latest source timestamp that was successfully
processed.

``` text
Entity       Last Processed Timestamp
-----------  -------------------------
customers    2026-08-18 09:05:00
products     2026-08-18 09:06:00
orders       2026-08-18 09:11:00
```

When the next batch arrives, the pipeline compares incoming records with
the stored watermark.

``` text
Previous watermark: 2026-08-18 09:05

Incoming records:
08:55  Skip
09:00  Skip
09:05  Skip
09:10  Process
09:15  Process

New watermark: 2026-08-18 09:15
```

Only records newer than the previous watermark are considered for
incremental processing.

  -----------------------------------------------------------------------
  Column                              Purpose
  ----------------------------------- -----------------------------------
  `entity_name`                       Entity being tracked

  `watermark_value`                   Latest source timestamp
                                      successfully processed

  `last_batch_id`                     Batch responsible for the latest
                                      watermark

  `last_success_utc`                  Timestamp of the last successful
                                      processing operation
  -----------------------------------------------------------------------

This prevents the pipeline from unnecessarily reprocessing the complete
dataset on every execution.

The watermark works together with Delta Lake `MERGE` and stable business
keys to support **idempotent processing**. This means the same batch can
safely be rerun without creating duplicate business records.

## 3. Pipeline Run Audit --- `ctl_pipeline_run_audit`

The `ctl_pipeline_run_audit` table provides operational monitoring and
execution history for the incremental data pipeline.

Every pipeline execution receives a unique `run_id`. When processing
begins, an audit record is created with `Status = RUNNING`. When
processing completes successfully, the record is updated to
`Status = SUCCEEDED`. If an exception occurs, the run is recorded as
`Status = FAILED` together with the associated error information.

  Column                 Purpose
  ---------------------- --------------------------------------------------
  `run_id`               Unique identifier for the execution
  `pipeline_name`        Pipeline that initiated the processing
  `batch_id`             Batch being processed
  `environment`          DEV, TEST, or PROD
  `start_utc`            Processing start time
  `end_utc`              Processing completion time
  `status`               RUNNING, SUCCEEDED, or FAILED
  `entities_processed`   Number of entities processed
  `rows_inserted`        Number of new records inserted
  `rows_updated`         Number of existing records updated
  `rows_rejected`        Number of records rejected by data-quality rules
  `error_message`        Error details when processing fails

Example successful execution:

``` text
Pipeline:           PL_02_Retail_Incremental_DataOps
Environment:        DEV
Batch:              20260818_01
Status:             SUCCEEDED
Entities Processed: 6
Rows Inserted:      7
Rows Updated:       1
Rows Rejected:      1
```

This provides an operational history that can be queried when
troubleshooting pipeline failures or validating data loads. The
engineering team can determine which batch was processed, when it ran,
whether it succeeded, how many records were inserted or updated, how
many failed validation, and what error caused a failed execution.

## How the Three Control Tables Work Together

``` text
                 ctl_entity_config
                         |
                         | What should be processed?
                         v
                 Incremental Pipeline
                         |
                         v
               ctl_entity_watermark
                         |
                         | Where did processing
                         | finish last time?
                         v
                  Read New Data
                         |
                         v
                Validate Records
                    /         \
                   /           \
               Valid          Invalid
                 |               |
                 v               v
            Delta MERGE      Quarantine
                 |
                 v
          Update Watermark
                 |
                 v
        ctl_pipeline_run_audit
                 |
                 | What happened?
                 v
       Operational Monitoring
```

  -----------------------------------------------------------------------
  Control Table                       Question It Answers
  ----------------------------------- -----------------------------------
  `ctl_entity_config`                 **What should the pipeline process
                                      and how?**

  `ctl_entity_watermark`              **Where did the previous successful
                                      incremental load finish?**

  `ctl_pipeline_run_audit`            **What happened during each
                                      pipeline execution?**
  -----------------------------------------------------------------------

This control framework moves the solution beyond a basic ETL pipeline by
introducing metadata management, incremental-state tracking, operational
observability, auditability, and failure diagnostics---capabilities
required for maintaining reliable production data platforms.
