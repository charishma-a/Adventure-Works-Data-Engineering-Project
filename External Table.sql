create DATABASE SCOPED CREDENTIAL cred_cherry
WITH
    IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://datalakecherry.blob.core.windows.net/sillver',
    CREDENTIAL = cred_cherry
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://datalakecherry.blob.core.windows.net/gold',
    CREDENTIAL = cred_cherry
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

-- create EXTERNAL TABLE EXTSALES
----------------------------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales

SELECT * FROM gold.extsales
