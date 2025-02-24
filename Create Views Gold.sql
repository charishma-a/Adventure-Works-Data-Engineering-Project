-- Create View Calendar
-------------------------
Create VIEW gold.calendar
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/AdventureWorks_Calendar/',
        FORMAT = 'PARQUET'
    ) as query1

-- Create View Customers
-------------------------
Create VIEW gold.customers
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/AdventureWorks_Customers/',
        FORMAT = 'PARQUET'
    ) as query1

-- Create View Products
-------------------------
Create VIEW gold.products
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/AdventureWorks_Products/',
        FORMAT = 'PARQUET'
    ) as query1


-- Create View Returns
-------------------------
Create VIEW gold.returns
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/AdventureWorks_Returns/',
        FORMAT = 'PARQUET'
    ) as query1


-- Create View Sales
-------------------------
Create VIEW gold.sales
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/AdventureWorks_Sales/',
        FORMAT = 'PARQUET'
    ) as query1


-- Create View Territories
-------------------------
Create VIEW gold.territories
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/AdventureWorks_Territories/',
        FORMAT = 'PARQUET'
    ) as query1 


-- Create View ProductSubcat
-----------------------------
Create VIEW gold.productsubcat
AS
select * from
    OPENROWSET(
        BULK 'https://datalakecherry.blob.core.windows.net/sillver/Product_Subcategories/',
        FORMAT = 'PARQUET'
    ) as query1       