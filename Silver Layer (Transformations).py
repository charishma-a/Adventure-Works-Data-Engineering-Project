# Databricks notebook source
# MAGIC %md
# MAGIC # TRANSFORMATION SCRIPTS

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA LOADING

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakecherry.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalakecherry.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalakecherry.dfs.core.windows.net", "8eaf2fb3-46dd-4f03-a5ad-ffb643dd0583")
spark.conf.set("fs.azure.account.oauth2.client.secret.datalakecherry.dfs.core.windows.net", "NJE8Q~iqbrRJL8KK7MRHpltB-3rhSVhC_YF5OdcW")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakecherry.dfs.core.windows.net", "https://login.microsoftonline.com/7953a50a-3b12-4ec0-a851-d80c00cf756d/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Data**
# MAGIC

# COMMAND ----------

df = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df.display()

# COMMAND ----------

df_cus = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_prod_cat = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_products = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Products')
df_products.display()

# COMMAND ----------

df_returns = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Returns')
df_returns.display()

# COMMAND ----------

df_sales = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Sales*')
df_sales.display()

# COMMAND ----------

df_ter = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/AdventureWorks_Territories')
df_ter.display()

# COMMAND ----------

df_sub_cat = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@datalakecherry.dfs.core.windows.net/Product_Subcategories')
df_sub_cat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **# **TRANSFORMATIONS****

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATE FUNCTIONS

# COMMAND ----------

df_cal = df.withColumn('Month', month(col('Date')))\
            .withColumn('Year', year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/AdventureWorks_Calendar').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CUSTOMERS

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn('FullName', concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/AdventureWorks_Customers').save()

# COMMAND ----------

df_sub_cat.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/Product_Subcategories').save()

# COMMAND ----------

df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Get the first 2 letters from ProductSKU
# MAGIC 2. Get the first word from ProductName

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
                          .withColumn('ProductName', split(col('ProductName'), ',')[0])

# COMMAND ----------

df_products.display()

# COMMAND ----------



# COMMAND ----------

df_products.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/AdventureWorks_Products').save()

# COMMAND ----------

df_returns.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/AdventureWorks_Returns').save()

# COMMAND ----------

df_ter.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/AdventureWorks_Territories').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sales**

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Analysis

# COMMAND ----------

#We want to find out the count of order numbers per each day. 
df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('OrderCount')).display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_sales.write.format('parquet').mode('append').option('path', 'abfss://sillver@datalakecherry.dfs.core.windows.net/AdventureWorks_Sales').save()