# Idempotency



The pipeline is designed so that replaying the same batch does not

create duplicate records.



This is achieved using:



1\. SourceUpdatedAt watermarks.

2\. Stable business keys.

3\. Delta Lake MERGE.

4\. Warehouse MERGE.

5\. Batch-aware quarantine logic.

