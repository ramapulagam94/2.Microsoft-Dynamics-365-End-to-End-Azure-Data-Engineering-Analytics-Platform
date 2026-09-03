# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,function to read each entity
base_path = "abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net/raw"

def readFromDeltaPath(entityName):
    df = spark.read.format("delta")\
               .load(f"{base_path}/{entityName}")
    return df

# COMMAND ----------

# DBTITLE 1,Save the deltatable to catalog
def saveToDeltaToCatalog(df, catalog, schema, tableName):

    schema =schema.lower() #convert to lower case
    tableName = tableName.lower() #convert to lower case
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}") # Create schema if not exists
    df.write.format("delta")\
           .mode("overwrite")\
           .saveAsTable(f"{catalog}.{schema}.{tableName}")

# COMMAND ----------

def appendToDeltaTable(df, catalog, schema, tableName):

    schema =schema.lower() #convert to lower case
    tableName = tableName.lower() #convert to lower case
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}") # Create schema if not exists
    df.write.format("delta")\
           .mode("append")\
           .option("mergeSchema", "true")\
           .saveAsTable(f"{catalog}.{schema}.{tableName}")

# COMMAND ----------

# DBTITLE 1,Import all the pysaprk functions
import pyspark.sql.functions as F
import datetime
import pyspark.sql.types as T
import dateutil
import pandas as pd