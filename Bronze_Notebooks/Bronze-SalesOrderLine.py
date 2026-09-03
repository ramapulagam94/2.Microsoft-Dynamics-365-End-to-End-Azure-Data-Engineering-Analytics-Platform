# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Run the shared libraries for UDFs
# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

# DBTITLE 1,read data from ADLS raw
df_SalesOrderLine = readFromDeltaPath("Sales/SalesOrderLine")
display(df_SalesOrderLine)

# COMMAND ----------

# DBTITLE 1,write to UC-bronze layer
saveToDeltaToCatalog(df_SalesOrderLine, "dev_catalog", "bronze", "SalesOrderLine")

# COMMAND ----------

# DBTITLE 1,list of tables in bronze layer
# MAGIC %sql
# MAGIC SHOW TABLES IN dev_Catalog.bronze;
# MAGIC

# COMMAND ----------

# DBTITLE 1,read from UC-bronze to confirm
df = spark.read.table("dev_catalog.bronze.salesorderline")
display(df)