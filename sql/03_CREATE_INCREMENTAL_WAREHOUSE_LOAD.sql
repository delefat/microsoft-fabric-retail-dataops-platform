CREATE OR ALTER PROCEDURE dbo.usp_Load_Retail_Warehouse
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MaxCustomerKey BIGINT = COALESCE((SELECT MAX(CustomerKey) FROM dbo.DimCustomer),0);
    WITH src AS (
      SELECT CustomerID,CustomerName,Email,City,Region,CustomerType,CreatedDate,
             @MaxCustomerKey + ROW_NUMBER() OVER(ORDER BY CustomerID) AS NewKey
      FROM LH_Retail_Lakehouse.dbo.silver_customers)
    MERGE dbo.DimCustomer AS t USING src AS s ON t.CustomerID=s.CustomerID
    WHEN MATCHED THEN UPDATE SET CustomerName=s.CustomerName,Email=s.Email,City=s.City,Region=s.Region,
                                 CustomerType=s.CustomerType,CreatedDate=s.CreatedDate
    WHEN NOT MATCHED THEN INSERT(CustomerKey,CustomerID,CustomerName,Email,City,Region,CustomerType,CreatedDate)
      VALUES(s.NewKey,s.CustomerID,s.CustomerName,s.Email,s.City,s.Region,s.CustomerType,s.CreatedDate);

    DECLARE @MaxProductKey BIGINT = COALESCE((SELECT MAX(ProductKey) FROM dbo.DimProduct),0);
    WITH src AS (
      SELECT ProductID,ProductName,Category,UnitCost,UnitPrice,IsActive,
             @MaxProductKey + ROW_NUMBER() OVER(ORDER BY ProductID) AS NewKey
      FROM LH_Retail_Lakehouse.dbo.silver_products)
    MERGE dbo.DimProduct AS t USING src AS s ON t.ProductID=s.ProductID
    WHEN MATCHED THEN UPDATE SET ProductName=s.ProductName,Category=s.Category,UnitCost=s.UnitCost,UnitPrice=s.UnitPrice,IsActive=s.IsActive
    WHEN NOT MATCHED THEN INSERT(ProductKey,ProductID,ProductName,Category,UnitCost,UnitPrice,IsActive)
      VALUES(s.NewKey,s.ProductID,s.ProductName,s.Category,s.UnitCost,s.UnitPrice,s.IsActive);

    DECLARE @MaxStoreKey BIGINT = COALESCE((SELECT MAX(StoreKey) FROM dbo.DimStore),0);
    WITH src AS (
      SELECT StoreID,StoreName,City,Region,@MaxStoreKey + ROW_NUMBER() OVER(ORDER BY StoreID) AS NewKey
      FROM LH_Retail_Lakehouse.dbo.silver_stores)
    MERGE dbo.DimStore AS t USING src AS s ON t.StoreID=s.StoreID
    WHEN MATCHED THEN UPDATE SET StoreName=s.StoreName,City=s.City,Region=s.Region
    WHEN NOT MATCHED THEN INSERT(StoreKey,StoreID,StoreName,City,Region)
      VALUES(s.NewKey,s.StoreID,s.StoreName,s.City,s.Region);

    WITH src AS (
      SELECT DISTINCT YEAR(OrderDate)*10000+MONTH(OrderDate)*100+DAY(OrderDate) DateKey,
             OrderDate FullDate,YEAR(OrderDate) CalendarYear,MONTH(OrderDate) CalendarMonth,
             DAY(OrderDate) DayOfMonth,DATEPART(QUARTER,OrderDate) CalendarQuarter
      FROM LH_Retail_Lakehouse.dbo.silver_orders)
    MERGE dbo.DimDate AS t USING src AS s ON t.DateKey=s.DateKey
    WHEN NOT MATCHED THEN INSERT(DateKey,FullDate,CalendarYear,CalendarMonth,DayOfMonth,CalendarQuarter)
      VALUES(s.DateKey,s.FullDate,s.CalendarYear,s.CalendarMonth,s.DayOfMonth,s.CalendarQuarter);

    WITH src AS (
      SELECT oi.OrderItemID,oi.OrderID,dc.CustomerKey,dp.ProductKey,ds.StoreKey,dd.DateKey,
             oi.Quantity,oi.UnitPrice,oi.DiscountAmount,oi.GrossAmount,oi.NetAmount
      FROM LH_Retail_Lakehouse.dbo.silver_order_items oi
      JOIN LH_Retail_Lakehouse.dbo.silver_orders o ON oi.OrderID=o.OrderID
      JOIN dbo.DimCustomer dc ON o.CustomerID=dc.CustomerID
      JOIN dbo.DimProduct dp ON oi.ProductID=dp.ProductID
      JOIN dbo.DimStore ds ON o.StoreID=ds.StoreID
      JOIN dbo.DimDate dd ON o.OrderDate=dd.FullDate)
    MERGE dbo.FactSales AS t USING src AS s ON t.OrderItemID=s.OrderItemID
    WHEN MATCHED THEN UPDATE SET OrderID=s.OrderID,CustomerKey=s.CustomerKey,ProductKey=s.ProductKey,
      StoreKey=s.StoreKey,DateKey=s.DateKey,Quantity=s.Quantity,UnitPrice=s.UnitPrice,
      DiscountAmount=s.DiscountAmount,GrossAmount=s.GrossAmount,NetAmount=s.NetAmount
    WHEN NOT MATCHED THEN INSERT(OrderItemID,OrderID,CustomerKey,ProductKey,StoreKey,DateKey,Quantity,UnitPrice,DiscountAmount,GrossAmount,NetAmount)
      VALUES(s.OrderItemID,s.OrderID,s.CustomerKey,s.ProductKey,s.StoreKey,s.DateKey,s.Quantity,s.UnitPrice,s.DiscountAmount,s.GrossAmount,s.NetAmount);

    WITH src AS (
      SELECT ds.StoreKey,dp.ProductKey,YEAR(i.SnapshotDate)*10000+MONTH(i.SnapshotDate)*100+DAY(i.SnapshotDate) SnapshotDateKey,
             i.QuantityOnHand,i.ReorderLevel
      FROM LH_Retail_Lakehouse.dbo.silver_inventory i
      JOIN dbo.DimStore ds ON i.StoreID=ds.StoreID
      JOIN dbo.DimProduct dp ON i.ProductID=dp.ProductID)
    MERGE dbo.FactInventory AS t USING src AS s
      ON t.StoreKey=s.StoreKey AND t.ProductKey=s.ProductKey AND t.SnapshotDateKey=s.SnapshotDateKey
    WHEN MATCHED THEN UPDATE SET QuantityOnHand=s.QuantityOnHand,ReorderLevel=s.ReorderLevel
    WHEN NOT MATCHED THEN INSERT(StoreKey,ProductKey,SnapshotDateKey,QuantityOnHand,ReorderLevel)
      VALUES(s.StoreKey,s.ProductKey,s.SnapshotDateKey,s.QuantityOnHand,s.ReorderLevel);
END;
