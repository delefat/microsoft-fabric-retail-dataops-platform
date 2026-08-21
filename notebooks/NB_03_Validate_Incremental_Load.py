from pyspark.sql import functions as F
assert spark.table("silver_customers").filter("CustomerID='C0101'").count()==1
assert spark.table("silver_products").filter("ProductID='P0031'").count()==1
assert spark.table("silver_stores").filter("StoreID='S006'").count()==1
assert spark.table("silver_orders").filter("OrderID='O000501'").count()==1
assert spark.table("silver_order_items").filter("OrderItemID='OI2000001'").count()==1
assert spark.table("silver_order_items").filter("OrderItemID='OI2000999'").count()==0
assert spark.table("quarantine_order_items").filter("OrderItemID='OI2000999'").count()>=1
assert spark.table("ctl_entity_watermark").filter("watermark_value IS NULL").count()==0
assert spark.table("ctl_pipeline_run_audit").orderBy(F.col("start_utc").desc()).first()["status"]=="SUCCEEDED"
print("All Phase 2 incremental tests passed")
display(spark.table("ctl_entity_watermark").orderBy("entity_name"))
display(spark.table("ctl_pipeline_run_audit").orderBy(F.col("start_utc").desc()).limit(10))
