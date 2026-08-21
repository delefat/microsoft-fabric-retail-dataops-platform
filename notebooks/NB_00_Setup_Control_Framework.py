# NB_00_Setup_Control_Framework
from pyspark.sql.types import *
from pyspark.sql import functions as F

entities=["customers","products","stores","orders","order_items","inventory"]

if not spark.catalog.tableExists("ctl_entity_watermark"):
    schema="entity_name string, watermark_value timestamp, last_batch_id string, last_success_utc timestamp"
    spark.createDataFrame([(e,None,None,None) for e in entities],schema).write.format("delta").saveAsTable("ctl_entity_watermark")

if not spark.catalog.tableExists("ctl_pipeline_run_audit"):
    schema="""run_id string, pipeline_name string, batch_id string, environment string,
              start_utc timestamp, end_utc timestamp, status string, entities_processed int,
              rows_inserted long, rows_updated long, rows_rejected long, error_message string"""
    spark.createDataFrame([],schema).write.format("delta").saveAsTable("ctl_pipeline_run_audit")

if not spark.catalog.tableExists("ctl_entity_config"):
    rows=[
      ("customers","silver_customers","CustomerID","_source_updated_at",True,10),
      ("products","silver_products","ProductID","_source_updated_at",True,20),
      ("stores","silver_stores","StoreID","_source_updated_at",True,30),
      ("orders","silver_orders","OrderID","_source_updated_at",True,40),
      ("order_items","silver_order_items","OrderItemID","_source_updated_at",True,50),
      ("inventory","silver_inventory","StoreID|ProductID|SnapshotDate","_source_updated_at",True,60)]
    schema="entity_name string,target_table string,business_key string,watermark_column string,load_enabled boolean,load_order int"
    spark.createDataFrame(rows,schema).write.format("delta").saveAsTable("ctl_entity_config")

for t in ["silver_customers","silver_products","silver_stores","silver_orders","silver_order_items","silver_inventory"]:
    if spark.catalog.tableExists(t):
        cols=spark.table(t).columns
        if "_source_updated_at" not in cols:
            spark.sql(f"ALTER TABLE {t} ADD COLUMNS (_source_updated_at TIMESTAMP)")
            spark.sql(f"UPDATE {t} SET _source_updated_at=COALESCE(_load_timestamp,current_timestamp()) WHERE _source_updated_at IS NULL")
        if "_batch_id" not in spark.table(t).columns:
            spark.sql(f"ALTER TABLE {t} ADD COLUMNS (_batch_id STRING)")
            spark.sql(f"UPDATE {t} SET _batch_id='PHASE1_BASELINE' WHERE _batch_id IS NULL")

for t in ["quarantine_orders","quarantine_order_items"]:
    if spark.catalog.tableExists(t) and "_batch_id" not in spark.table(t).columns:
        spark.sql(f"ALTER TABLE {t} ADD COLUMNS (_batch_id STRING)")
        spark.sql(f"UPDATE {t} SET _batch_id='PHASE1_BASELINE' WHERE _batch_id IS NULL")

print("Control framework ready")
display(spark.table("ctl_entity_config"))
display(spark.table("ctl_entity_watermark"))
