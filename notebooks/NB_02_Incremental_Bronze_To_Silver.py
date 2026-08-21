# NB_02_Incremental_Bronze_To_Silver
# Attach LH_Retail_Lakehouse as the default lakehouse.
# Mark the following four variables as a Fabric Parameters cell.
environment = "DEV"
batch_id = "20260818_01"
pipeline_name = "PL_02_Retail_Incremental_DataOps"
bronze_root = "Files/bronze/incremental"

from datetime import datetime, timezone
import uuid
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *

run_id = str(uuid.uuid4())
run_start = datetime.now(timezone.utc).replace(tzinfo=None)

def append_audit_start():
    schema = """run_id string,pipeline_name string,batch_id string,environment string,
                start_utc timestamp,end_utc timestamp,status string,entities_processed int,
                rows_inserted long,rows_updated long,rows_rejected long,error_message string"""
    row=[(run_id,pipeline_name,batch_id,environment,run_start,None,"RUNNING",0,0,0,0,None)]
    spark.createDataFrame(row,schema).write.format("delta").mode("append").saveAsTable("ctl_pipeline_run_audit")

def finish_audit(status,entities,inserted,updated,rejected,error=None):
    DeltaTable.forName(spark,"ctl_pipeline_run_audit").update(
        condition=F.col("run_id")==run_id,
        set={"end_utc":F.lit(datetime.now(timezone.utc).replace(tzinfo=None)),
             "status":F.lit(status),"entities_processed":F.lit(int(entities)),
             "rows_inserted":F.lit(int(inserted)),"rows_updated":F.lit(int(updated)),
             "rows_rejected":F.lit(int(rejected)),"error_message":F.lit(error)})

def get_watermark(entity):
    row=(spark.table("ctl_entity_watermark").filter(F.col("entity_name")==entity)
         .select("watermark_value").first())
    return row["watermark_value"] if row else None

def set_watermark(entity,value):
    src=spark.createDataFrame([(entity,value,batch_id,datetime.now(timezone.utc).replace(tzinfo=None))],
        "entity_name string,watermark_value timestamp,last_batch_id string,last_success_utc timestamp")
    (DeltaTable.forName(spark,"ctl_entity_watermark").alias("t")
     .merge(src.alias("s"),"t.entity_name=s.entity_name")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def merge_delta(table,df,keys):
    if df.limit(1).count()==0: return 0,0
    target=DeltaTable.forName(spark,table)
    pred=" AND ".join([f"t.`{k}`=s.`{k}`" for k in keys])
    (target.alias("t").merge(df.alias("s"),pred)
     .whenMatchedUpdateAll(condition="s._source_updated_at > t._source_updated_at")
     .whenNotMatchedInsertAll().execute())
    m=target.history(1).select("operationMetrics").first()[0]
    return int(m.get("numTargetRowsInserted",0)),int(m.get("numTargetRowsUpdated",0))

def merge_quarantine(table,df,keys):
    if df.limit(1).count()==0: return
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table); return
    pred=" AND ".join([f"t.`{k}`=s.`{k}`" for k in keys]+["t._batch_id=s._batch_id"])
    (DeltaTable.forName(spark,table).alias("t").merge(df.alias("s"),pred)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def read_batch(entity,schema):
    path=f"{bronze_root}/{batch_id}/{entity}/{entity}.csv"
    return (spark.read.option("header","true").schema(schema).csv(path)
            .withColumnRenamed("SourceUpdatedAt","_source_updated_at")
            .withColumn("_source_file",F.lit(f"{entity}.csv"))
            .withColumn("_load_timestamp",F.current_timestamp())
            .withColumn("_batch_id",F.lit(batch_id)))

def newer_than_watermark(entity,df):
    wm=get_watermark(entity)
    return df if wm is None else df.filter(F.col("_source_updated_at")>F.lit(wm))

customer_schema=StructType([
 StructField("CustomerID",StringType()),StructField("CustomerName",StringType()),
 StructField("Email",StringType()),StructField("City",StringType()),StructField("Region",StringType()),
 StructField("CustomerType",StringType()),StructField("CreatedDate",DateType()),StructField("SourceUpdatedAt",TimestampType())])
product_schema=StructType([
 StructField("ProductID",StringType()),StructField("ProductName",StringType()),StructField("Category",StringType()),
 StructField("UnitCost",DecimalType(12,2)),StructField("UnitPrice",DecimalType(12,2)),StructField("IsActive",IntegerType()),
 StructField("SourceUpdatedAt",TimestampType())])
store_schema=StructType([
 StructField("StoreID",StringType()),StructField("StoreName",StringType()),StructField("City",StringType()),
 StructField("Region",StringType()),StructField("SourceUpdatedAt",TimestampType())])
order_schema=StructType([
 StructField("OrderID",StringType()),StructField("CustomerID",StringType()),StructField("StoreID",StringType()),
 StructField("OrderDate",DateType()),StructField("OrderStatus",StringType()),StructField("SourceUpdatedAt",TimestampType())])
item_schema=StructType([
 StructField("OrderItemID",StringType()),StructField("OrderID",StringType()),StructField("ProductID",StringType()),
 StructField("Quantity",IntegerType()),StructField("UnitPrice",DecimalType(12,2)),StructField("DiscountAmount",DecimalType(12,2)),
 StructField("SourceUpdatedAt",TimestampType())])
inv_schema=StructType([
 StructField("StoreID",StringType()),StructField("ProductID",StringType()),StructField("SnapshotDate",DateType()),
 StructField("QuantityOnHand",IntegerType()),StructField("ReorderLevel",IntegerType()),StructField("SourceUpdatedAt",TimestampType())])

append_audit_start()
ins_total=upd_total=rej_total=entities=0
try:
    customers=newer_than_watermark("customers",read_batch("customers",customer_schema))
    valid=customers.filter("CustomerID IS NOT NULL AND CustomerName IS NOT NULL").dropDuplicates(["CustomerID"])
    i,u=merge_delta("silver_customers",valid,["CustomerID"]); ins_total+=i; upd_total+=u
    if customers.limit(1).count(): set_watermark("customers",customers.agg(F.max("_source_updated_at")).first()[0])
    entities+=1

    products=newer_than_watermark("products",read_batch("products",product_schema))
    valid=products.filter("ProductID IS NOT NULL AND UnitPrice >= 0").dropDuplicates(["ProductID"])
    i,u=merge_delta("silver_products",valid,["ProductID"]); ins_total+=i; upd_total+=u
    if products.limit(1).count(): set_watermark("products",products.agg(F.max("_source_updated_at")).first()[0])
    entities+=1

    stores=newer_than_watermark("stores",read_batch("stores",store_schema))
    valid=stores.filter("StoreID IS NOT NULL").dropDuplicates(["StoreID"])
    i,u=merge_delta("silver_stores",valid,["StoreID"]); ins_total+=i; upd_total+=u
    if stores.limit(1).count(): set_watermark("stores",stores.agg(F.max("_source_updated_at")).first()[0])
    entities+=1

    valid_customers=spark.table("silver_customers").select("CustomerID").distinct()
    valid_stores=spark.table("silver_stores").select("StoreID").distinct()
    valid_products=spark.table("silver_products").select("ProductID").distinct()

    orders=newer_than_watermark("orders",read_batch("orders",order_schema))
    checked=(orders.join(valid_customers.withColumn("_customer_exists",F.lit(1)),"CustomerID","left")
      .join(valid_stores.withColumn("_store_exists",F.lit(1)),"StoreID","left")
      .withColumn("validation_error",
        F.when(F.col("OrderID").isNull(),"OrderID is NULL")
         .when(F.col("CustomerID").isNull(),"CustomerID is NULL")
         .when(F.col("_customer_exists").isNull(),"CustomerID not found")
         .when(F.col("_store_exists").isNull(),"StoreID not found")
         .when(F.col("OrderDate").isNull(),"OrderDate is NULL")))
    good=checked.filter("validation_error IS NULL").drop("_customer_exists","_store_exists","validation_error").dropDuplicates(["OrderID"])
    bad=checked.filter("validation_error IS NOT NULL").drop("_customer_exists","_store_exists")
    rej_total+=bad.count(); merge_quarantine("quarantine_orders",bad,["OrderID"])
    i,u=merge_delta("silver_orders",good,["OrderID"]); ins_total+=i; upd_total+=u
    if orders.limit(1).count(): set_watermark("orders",orders.agg(F.max("_source_updated_at")).first()[0])
    entities+=1

    items=newer_than_watermark("order_items",read_batch("order_items",item_schema))
    valid_orders=spark.table("silver_orders").select("OrderID").distinct()
    checked=(items.join(valid_orders.withColumn("_order_exists",F.lit(1)),"OrderID","left")
      .join(valid_products.withColumn("_product_exists",F.lit(1)),"ProductID","left")
      .withColumn("validation_error",
        F.when(F.col("OrderItemID").isNull(),"OrderItemID is NULL")
         .when(F.col("_order_exists").isNull(),"OrderID not found")
         .when(F.col("_product_exists").isNull(),"ProductID not found")
         .when(F.col("Quantity").isNull()|(F.col("Quantity")<=0),"Quantity must be > 0")
         .when(F.col("UnitPrice").isNull()|(F.col("UnitPrice")<0),"UnitPrice must be >= 0")))
    good=(checked.filter("validation_error IS NULL").drop("_order_exists","_product_exists","validation_error")
          .withColumn("GrossAmount",F.col("Quantity")*F.col("UnitPrice"))
          .withColumn("NetAmount",F.col("Quantity")*F.col("UnitPrice")-F.coalesce(F.col("DiscountAmount"),F.lit(0)))
          .dropDuplicates(["OrderItemID"]))
    bad=checked.filter("validation_error IS NOT NULL").drop("_order_exists","_product_exists")
    rej_total+=bad.count(); merge_quarantine("quarantine_order_items",bad,["OrderItemID"])
    i,u=merge_delta("silver_order_items",good,["OrderItemID"]); ins_total+=i; upd_total+=u
    if items.limit(1).count(): set_watermark("order_items",items.agg(F.max("_source_updated_at")).first()[0])
    entities+=1

    inv=newer_than_watermark("inventory",read_batch("inventory",inv_schema))
    good=(inv.join(valid_stores,"StoreID","inner").join(valid_products,"ProductID","inner")
          .filter("QuantityOnHand >= 0").dropDuplicates(["StoreID","ProductID","SnapshotDate"]))
    i,u=merge_delta("silver_inventory",good,["StoreID","ProductID","SnapshotDate"]); ins_total+=i; upd_total+=u
    if inv.limit(1).count(): set_watermark("inventory",inv.agg(F.max("_source_updated_at")).first()[0])
    entities+=1

    finish_audit("SUCCEEDED",entities,ins_total,upd_total,rej_total)
    print(f"SUCCEEDED run_id={run_id} inserted={ins_total} updated={upd_total} rejected={rej_total}")
except Exception as e:
    finish_audit("FAILED",entities,ins_total,upd_total,rej_total,str(e)[:4000])
    raise
