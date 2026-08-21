-- 01_BUILD_RETAIL_WAREHOUSE.sql
-- Run this in WH_Retail after adding the LH_Retail_Lakehouse SQL analytics endpoint
-- to the Warehouse Explorer. Cross-database three-part names require both items
-- to be in the same Fabric workspace.

DROP TABLE IF EXISTS dbo.FactSales;
DROP TABLE IF EXISTS dbo.FactInventory;
DROP TABLE IF EXISTS dbo.DimCustomer;
DROP TABLE IF EXISTS dbo.DimProduct;
DROP TABLE IF EXISTS dbo.DimStore;
DROP TABLE IF EXISTS dbo.DimDate;

-- Customer dimension
CREATE TABLE dbo.DimCustomer
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CustomerID) AS CustomerKey,
    CustomerID,
    CustomerName,
    Email,
    City,
    Region,
    CustomerType,
    CreatedDate
FROM LH_Retail_Lakehouse.dbo.silver_customers;

-- Product dimension
CREATE TABLE dbo.DimProduct
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ProductID) AS ProductKey,
    ProductID,
    ProductName,
    Category,
    UnitCost,
    UnitPrice,
    IsActive
FROM LH_Retail_Lakehouse.dbo.silver_products;

-- Store dimension
CREATE TABLE dbo.DimStore
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY StoreID) AS StoreKey,
    StoreID,
    StoreName,
    City,
    Region
FROM LH_Retail_Lakehouse.dbo.silver_stores;

-- Date dimension, limited to dates present in orders for Phase 1
CREATE TABLE dbo.DimDate
AS
SELECT
    YEAR(OrderDate) * 10000 + MONTH(OrderDate) * 100 + DAY(OrderDate) AS DateKey,
    OrderDate AS FullDate,
    YEAR(OrderDate) AS CalendarYear,
    MONTH(OrderDate) AS CalendarMonth,
    DAY(OrderDate) AS DayOfMonth,
    DATEPART(QUARTER, OrderDate) AS CalendarQuarter
FROM LH_Retail_Lakehouse.dbo.silver_orders
GROUP BY OrderDate;

-- Sales fact: grain = one valid order line
CREATE TABLE dbo.FactSales
AS
SELECT
    oi.OrderItemID,
    oi.OrderID,
    dc.CustomerKey,
    dp.ProductKey,
    ds.StoreKey,
    dd.DateKey,
    oi.Quantity,
    oi.UnitPrice,
    oi.DiscountAmount,
    oi.GrossAmount,
    oi.NetAmount
FROM LH_Retail_Lakehouse.dbo.silver_order_items AS oi
INNER JOIN LH_Retail_Lakehouse.dbo.silver_orders AS o
    ON oi.OrderID = o.OrderID
INNER JOIN dbo.DimCustomer AS dc
    ON o.CustomerID = dc.CustomerID
INNER JOIN dbo.DimProduct AS dp
    ON oi.ProductID = dp.ProductID
INNER JOIN dbo.DimStore AS ds
    ON o.StoreID = ds.StoreID
INNER JOIN dbo.DimDate AS dd
    ON o.OrderDate = dd.FullDate;

-- Inventory fact: grain = store + product + snapshot date
CREATE TABLE dbo.FactInventory
AS
SELECT
    ds.StoreKey,
    dp.ProductKey,
    YEAR(i.SnapshotDate) * 10000 + MONTH(i.SnapshotDate) * 100 + DAY(i.SnapshotDate) AS SnapshotDateKey,
    i.QuantityOnHand,
    i.ReorderLevel
FROM LH_Retail_Lakehouse.dbo.silver_inventory AS i
INNER JOIN dbo.DimStore AS ds
    ON i.StoreID = ds.StoreID
INNER JOIN dbo.DimProduct AS dp
    ON i.ProductID = dp.ProductID;

-- Validation
SELECT 'DimCustomer' AS TableName, COUNT(*) AS RowCount FROM dbo.DimCustomer
UNION ALL SELECT 'DimProduct', COUNT(*) FROM dbo.DimProduct
UNION ALL SELECT 'DimStore', COUNT(*) FROM dbo.DimStore
UNION ALL SELECT 'DimDate', COUNT(*) FROM dbo.DimDate
UNION ALL SELECT 'FactSales', COUNT(*) FROM dbo.FactSales
UNION ALL SELECT 'FactInventory', COUNT(*) FROM dbo.FactInventory;
