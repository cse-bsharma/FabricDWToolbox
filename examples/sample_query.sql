-- Example query for testing the Fabric Query Tuner
-- This query has several potential performance issues for demonstration

-- Example 1: Simple query that might benefit from an index
SELECT 
    o.OrderID,
    o.OrderDate,
    o.CustomerID,
    c.CustomerName,
    c.Email,
    o.TotalAmount
FROM Orders o
INNER JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE o.OrderDate >= DATEADD(month, -3, GETDATE())
    AND o.Status = 'Completed'
ORDER BY o.OrderDate DESC;

-- Example 2: Query with potential table scan
/*
SELECT *
FROM LargeTransactionLog
WHERE YEAR(TransactionDate) = 2024
    AND TransactionType = 'SALE';
*/

-- Example 3: Query with multiple joins
/*
SELECT 
    p.ProductName,
    c.CategoryName,
    SUM(oi.Quantity) as TotalSold,
    SUM(oi.Quantity * oi.UnitPrice) as Revenue
FROM OrderItems oi
INNER JOIN Products p ON oi.ProductID = p.ProductID
INNER JOIN Categories c ON p.CategoryID = c.CategoryID
INNER JOIN Orders o ON oi.OrderID = o.OrderID
WHERE o.OrderDate >= '2024-01-01'
GROUP BY p.ProductName, c.CategoryName
HAVING SUM(oi.Quantity) > 100
ORDER BY Revenue DESC;
*/
