# NB_01_Bronze_To_Silver
# Attach LH_Retail_Lakehouse as the DEFAULT lakehouse before running.
# This notebook reads raw CSV files from Files/bronze and writes managed Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

RUN_ID = F.expr("uuid()")
LOAD_TS = F.current_timestamp()

# Explicit schemas: production-style ingestion should not rely on inference.
customer_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("CustomerType", StringType(), True),
    StructField("CreatedDate", DateType(), True),
])

product_schema = StructType([
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("UnitCost", DecimalType(12,2), True),
    StructField("UnitPrice", DecimalType(12,2), True),
    StructField("IsActive", IntegerType(), True),
])

store_schema = StructType([
    StructField("StoreID", StringType(), True),
    StructField("StoreName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Region", StringType(), True),
])

order_schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("StoreID", StringType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("OrderStatus", StringType(), True),
])

order_item_schema = StructType([
    StructField("OrderItemID", StringType(), True),
    StructField("OrderID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DecimalType(12,2), True),
    StructField("DiscountAmount", DecimalType(12,2), True),
])

inventory_schema = StructType([
    StructField("StoreID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("SnapshotDate", DateType(), True),
    StructField("QuantityOnHand", IntegerType(), True),
    StructField("ReorderLevel", IntegerType(), True),
])

def read_csv(path, schema):
    return (
        spark.read
             .option("header", "true")
             .schema(schema)
             .csv(path)
    )

def with_audit(df, source_file):
    return (
        df.withColumn("_source_file", F.lit(source_file))
          .withColumn("_load_timestamp", F.current_timestamp())
    )

customers = with_audit(read_csv("Files/bronze/customers/customers.csv", customer_schema), "customers.csv")
products = with_audit(read_csv("Files/bronze/products/products.csv", product_schema), "products.csv")
stores = with_audit(read_csv("Files/bronze/stores/stores.csv", store_schema), "stores.csv")
orders = with_audit(read_csv("Files/bronze/orders/orders.csv", order_schema), "orders.csv")
order_items = with_audit(read_csv("Files/bronze/order_items/order_items.csv", order_item_schema), "order_items.csv")
inventory = with_audit(read_csv("Files/bronze/inventory/inventory.csv", inventory_schema), "inventory.csv")

# ---------- MASTER DATA VALIDATION ----------
silver_customers = (
    customers
    .filter(F.col("CustomerID").isNotNull())
    .filter(F.col("CustomerName").isNotNull())
    .dropDuplicates(["CustomerID"])
)

silver_products = (
    products
    .filter(F.col("ProductID").isNotNull())
    .filter(F.col("UnitPrice") >= 0)
    .dropDuplicates(["ProductID"])
)

silver_stores = (
    stores
    .filter(F.col("StoreID").isNotNull())
    .dropDuplicates(["StoreID"])
)

# ---------- ORDERS: VALID / QUARANTINE ----------
valid_customer_ids = silver_customers.select("CustomerID").distinct()
valid_store_ids = silver_stores.select("StoreID").distinct()

orders_checked = (
    orders
    .join(valid_customer_ids.withColumn("_customer_exists", F.lit(1)), "CustomerID", "left")
    .join(valid_store_ids.withColumn("_store_exists", F.lit(1)), "StoreID", "left")
    .withColumn(
        "validation_error",
        F.when(F.col("OrderID").isNull(), F.lit("OrderID is NULL"))
         .when(F.col("CustomerID").isNull(), F.lit("CustomerID is NULL"))
         .when(F.col("_customer_exists").isNull(), F.lit("CustomerID not found"))
         .when(F.col("StoreID").isNull(), F.lit("StoreID is NULL"))
         .when(F.col("_store_exists").isNull(), F.lit("StoreID not found"))
         .when(F.col("OrderDate").isNull(), F.lit("OrderDate is NULL"))
    )
)

silver_orders = (
    orders_checked
    .filter(F.col("validation_error").isNull())
    .drop("_customer_exists", "_store_exists", "validation_error")
    .dropDuplicates(["OrderID"])
)

quarantine_orders = (
    orders_checked
    .filter(F.col("validation_error").isNotNull())
    .drop("_customer_exists", "_store_exists")
)

# ---------- ORDER ITEMS: VALID / QUARANTINE ----------
valid_order_ids = silver_orders.select("OrderID").distinct()
valid_product_ids = silver_products.select("ProductID").distinct()

items_checked = (
    order_items
    .join(valid_order_ids.withColumn("_order_exists", F.lit(1)), "OrderID", "left")
    .join(valid_product_ids.withColumn("_product_exists", F.lit(1)), "ProductID", "left")
    .withColumn(
        "validation_error",
        F.when(F.col("OrderItemID").isNull(), F.lit("OrderItemID is NULL"))
         .when(F.col("_order_exists").isNull(), F.lit("OrderID not found"))
         .when(F.col("_product_exists").isNull(), F.lit("ProductID not found"))
         .when(F.col("Quantity").isNull() | (F.col("Quantity") <= 0), F.lit("Quantity must be > 0"))
         .when(F.col("UnitPrice").isNull() | (F.col("UnitPrice") < 0), F.lit("UnitPrice must be >= 0"))
    )
)

silver_order_items = (
    items_checked
    .filter(F.col("validation_error").isNull())
    .drop("_order_exists", "_product_exists", "validation_error")
    .withColumn("GrossAmount", F.col("Quantity") * F.col("UnitPrice"))
    .withColumn("NetAmount", (F.col("Quantity") * F.col("UnitPrice")) - F.coalesce(F.col("DiscountAmount"), F.lit(0)))
    .dropDuplicates(["OrderItemID"])
)

quarantine_order_items = (
    items_checked
    .filter(F.col("validation_error").isNotNull())
    .drop("_order_exists", "_product_exists")
)

# ---------- INVENTORY ----------
silver_inventory = (
    inventory
    .join(valid_store_ids, "StoreID", "inner")
    .join(valid_product_ids, "ProductID", "inner")
    .filter(F.col("QuantityOnHand") >= 0)
    .dropDuplicates(["StoreID", "ProductID", "SnapshotDate"])
)

# ---------- WRITE DELTA TABLES ----------
tables = {
    "silver_customers": silver_customers,
    "silver_products": silver_products,
    "silver_stores": silver_stores,
    "silver_orders": silver_orders,
    "silver_order_items": silver_order_items,
    "silver_inventory": silver_inventory,
    "quarantine_orders": quarantine_orders,
    "quarantine_order_items": quarantine_order_items,
}

for table_name, df in tables.items():
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))

print("Silver load complete.")
for table_name in tables:
    print(table_name, spark.table(table_name).count())

# ---------- BASIC DATA QUALITY ASSERTIONS ----------
assert spark.table("silver_customers").filter("CustomerID IS NULL").count() == 0
assert spark.table("silver_products").filter("ProductID IS NULL").count() == 0
assert spark.table("silver_orders").filter("OrderID IS NULL").count() == 0
assert spark.table("silver_order_items").filter("Quantity <= 0").count() == 0

print("Phase 1 data-quality assertions passed.")
