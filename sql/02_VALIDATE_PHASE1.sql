-- 02_VALIDATE_PHASE1.sql

-- 1. No orphan dimensional keys in FactSales
SELECT COUNT(*) AS OrphanCustomerKeys
FROM dbo.FactSales f
LEFT JOIN dbo.DimCustomer d ON f.CustomerKey = d.CustomerKey
WHERE d.CustomerKey IS NULL;

SELECT COUNT(*) AS OrphanProductKeys
FROM dbo.FactSales f
LEFT JOIN dbo.DimProduct d ON f.ProductKey = d.ProductKey
WHERE d.ProductKey IS NULL;

SELECT COUNT(*) AS OrphanStoreKeys
FROM dbo.FactSales f
LEFT JOIN dbo.DimStore d ON f.StoreKey = d.StoreKey
WHERE d.StoreKey IS NULL;

-- 2. Business measures
SELECT
    SUM(Quantity) AS UnitsSold,
    SUM(GrossAmount) AS GrossSales,
    SUM(DiscountAmount) AS Discounts,
    SUM(NetAmount) AS NetSales
FROM dbo.FactSales;

-- 3. Sales by region
SELECT
    s.Region,
    SUM(f.NetAmount) AS NetSales
FROM dbo.FactSales f
JOIN dbo.DimStore s ON f.StoreKey = s.StoreKey
GROUP BY s.Region
ORDER BY NetSales DESC;

-- 4. Top products
SELECT TOP 10
    p.ProductName,
    SUM(f.Quantity) AS UnitsSold,
    SUM(f.NetAmount) AS NetSales
FROM dbo.FactSales f
JOIN dbo.DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName
ORDER BY NetSales DESC;
