# Databricks notebook source
# DBTITLE 1,Run the shared libraries for UDFs
# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

# DBTITLE 1,read data from ADLS raw
df_PurchContracts = readFromDeltaPath("Purchase/PurchContracts")
display(df_PurchContracts)

# COMMAND ----------

# DBTITLE 1,write to UC-bronze layer
saveToDeltaToCatalog(df_PurchContracts, "dev_catalog", "bronze", "PurchContracts")

# COMMAND ----------

# DBTITLE 1,list of tables in bronze layer
# MAGIC %sql
# MAGIC SHOW TABLES IN dev_Catalog.bronze;
# MAGIC

# COMMAND ----------

# DBTITLE 1,read from UC-bronze to confirm
df = spark.read.table("dev_catalog.bronze.purchcontracts")
display(df)