# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Shared Libraries
# MAGIC       ↓
# MAGIC Read Bronze salesorderline
# MAGIC       ↓
# MAGIC Get unique PaymentTypeDesc
# MAGIC       ↓
# MAGIC Read existing Silver dimpaymenttypes
# MAGIC       ↓
# MAGIC Find NEW payment types
# MAGIC       ↓
# MAGIC Find current MAX(PaymentTypeId)
# MAGIC       ↓
# MAGIC Generate temporary 1,2,3... IDs
# MAGIC       ↓
# MAGIC Add MAX existing ID
# MAGIC       ↓
# MAGIC Get final PaymentTypeId
# MAGIC       ↓
# MAGIC Append new rows to Silver
# MAGIC       ↓
# MAGIC Later: new payment type arrives in Bronze
# MAGIC       ↓
# MAGIC Run notebook again
# MAGIC       ↓
# MAGIC Only the new payment type is inserted
# MAGIC
# MAGIC Interview answer:
# MAGIC ------------------
# MAGIC "This is an incremental dimension-loading process. First, I read the payment types from the Bronze sales order table and remove duplicates. I then read the existing payment type dimension from Silver and use exceptAll to identify only the new payment types. I get the current maximum surrogate key from the Silver dimension, defaulting to zero if the table is empty. For the new records, I generate sequential row numbers using row_number(). I add the existing maximum ID to these row numbers so that the new records continue from the existing surrogate key sequence. Finally, I append only the new records to the Silver Delta table. When another payment type such as PayPal arrives in a later batch, the same process identifies PayPal as new, gets the current maximum ID, assigns the next ID, and appends it."
# MAGIC
# MAGIC Note: not using monotnonic ID because it's not guaranteed to be unique and sequential.
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC /*
# MAGIC | Your approach(exceptAll())                    | Delta-MERGE                                       |
# MAGIC | -------------------------------- | ------------------------------------------- |
# MAGIC | `exceptAll()` finds new records  | `MERGE` finds matching/non-matching records |
# MAGIC | `row_number()` generates IDs     | You still need an ID-generation strategy    |
# MAGIC | `MAX(id)` finds existing maximum | You may still need `MAX(id)`                |
# MAGIC | `append` inserts new records     | `MERGE` inserts atomically                  |
# MAGIC | Simple                           | More powerful                               |
# MAGIC | Good for insert-only dimension   | Good for insert/update/delete logic         |
# MAGIC
# MAGIC */

# COMMAND ----------

# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimpaymenttypes"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

df_salesorderline= spark.table("dev_catalog.bronze.salesorderline")
display(df_salesorderline)


# COMMAND ----------

# MAGIC %md ###Create silver dimension table  
# MAGIC --> DROP the table if you like to check code with empty table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --drop table if exists dev_catalog.silver.dimpaymenttypes
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.silver.dimpaymenttypes(
# MAGIC   PaymentTypeId INT,
# MAGIC   PaymentTypeDesc STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

df = df_salesorderline.select("PaymentTypeDesc").distinct()
display(df)

# COMMAND ----------

df_paymenttype = spark.table("dev_catalog.silver.dimpaymenttypes")
display(df_paymenttype)

# COMMAND ----------

#find the rows in df that are not present in df_paymenttype based on the columns being compared.
# df                    # 1 columns
# df_paymenttype.select  # 2 column

#Then exceptAll() will not work. Why? -exceptAll() requires both DataFrames to have the same number of columns and compatible data types.But here we are only selecting signle column from paymenttypes. Hence, works.

df_newrows=df.exceptAll(df_paymenttype.select("PaymentTypeDesc"))
display(df_newrows)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Interview answer:
# MAGIC "First, I query the df_paymenttype DataFrame to find the maximum existing PaymentTypeId. I use IFNULL to return 0 if the DataFrame is empty. The result is a Spark DataFrame containing one row with the maximum ID. Then I use head(1) to retrieve that row and extract the actual value using toprow[0][0]. Finally, I store that value in the maxid variable. This maximum ID can then be used as the starting point for generating IDs for newly identified payment types."
# MAGIC */

# COMMAND ----------

# DBTITLE 1,generate row numbers
#This code is getting the current maximum PaymentTypeId from df_paymenttype, and then storing that value in the Python variable maxid.

#IFNULL(MAX(PaymentTypeId), 0)--> handles when the table is empty

df_max = spark.sql("select ifnull(max(PaymentTypeId),0) as maxid from {df}",df=df_paymenttype)
toprow = df_max.head(1) #get first row
maxid = toprow[0][0] #Extract the value from the first column of the first row
print(maxid)

# COMMAND ----------

import pyspark.sql.window as W

# COMMAND ----------

#temporary sequential numbers--> generated by window row_number, later used

df_ids = df_newrows.withColumn("PaymentTypeId", F.row_number().over(window=W.Window.orderBy(F.col("PaymentTypeDesc"))))
display(df_ids)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Source
# MAGIC   ↓
# MAGIC Find new payment types
# MAGIC   ↓
# MAGIC exceptAll()
# MAGIC   ↓
# MAGIC Generate 1,2,3... for this batch
# MAGIC   ↓
# MAGIC Find existing MAX ID
# MAGIC   ↓
# MAGIC Add MAX ID
# MAGIC   ↓
# MAGIC Get final IDs
# MAGIC   ↓
# MAGIC Insert into target
# MAGIC
# MAGIC MAX(id) + row_number() does not itself guarantee uniqueness under concurrent writes.Both jobs think the next ID is 11.
# MAGIC
# MAGIC Better approach: use Delta + MERGE for the dimension
# MAGIC */

# COMMAND ----------

#This line is adding maxid to every PaymentTypeId in df_ids.

idsFinal = df_ids.withColumn("PaymentTypeId", F.col("PaymentTypeId")+maxid)
display(idsFinal)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from dev_catalog.silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = idsFinal

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

appendToDeltaTable(df_final,"dev_catalog","silver",Entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from dev_catalog.silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incrementally, adding one more payment type in bronze sales order -> Paypal

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.bronze.salesorderline(PaymentTypeDesc) values("PayPal")

# COMMAND ----------

# MAGIC %md
# MAGIC ### run the code beggining