# Databricks notebook source
# MAGIC %md
# MAGIC ### Requirement-1:

# COMMAND ----------

# MAGIC %md
# MAGIC - import pyspark.sql.functions as F (code in shared librabries)
# MAGIC - You will get datatypes of the columns (result from reading dfs)
# MAGIC - Trim the columns which are string type
# MAGIC - Timestamp columns to be converted to CST(The only recommendation is to use "America/Chicago" instead of "CST" because it automatically handles daylight saving time (CST/CDT).)
# MAGIC - Incase, date/timestamp columns are null, set the value to 1900-01-01
# MAGIC - ---------(coalesce() ✅ Most common for replacing NULL) or you can use when-otherwise
# MAGIC - ---------columns affected: lastmodifiedLastProcessedChange_DateTime, ValidTo
# MAGIC - Add a column named "UpdatedDateTime" which has current timestamp of execution on the final df. (withcolumn and current_timestamp)
# MAGIC - Remove rows with nulls on the basis of recordId. (use filter at the beginning)
# MAGIC - Add hash key column based on the PartyRecorId using xxhash64.
# MAGIC - ----------The hask key vaules are also -ve: This is expected behavior. xxhash64() in Spark returns a 64-bit signed integer (LongType), and signed integers can be positive or negative.
# MAGIC
# MAGIC Where is a hash key useful?--->
# MAGIC A hash key is useful for:-->
# MAGIC - Detecting changed records in SCD Type 1/2.
# MAGIC - Comparing many columns efficiently.
# MAGIC - Deduplication.
# MAGIC - Data validation.
# MAGIC
# MAGIC Note: we can also use Surrogate keys for unique record ids.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Run the shared libraries
# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

# DBTITLE 1,Read bronze tables from UC bronze
df_currency = spark.read.table("dev_catalog.bronze.currency")
#we get schema/data types of the above tables

# COMMAND ----------

# DBTITLE 1,Build Dimension table
dimCurrency_df = df_currency.filter(df_currency.CurrencyId.isNotNull()
                        ).select(
                        df_currency.CurrencyId,
                        F.trim(df_currency.Code).alias("CurrencyCode"),
                        F.coalesce(df_currency.LastProcessedChange_DateTime, 
                                   F.lit("1900-01-01 00:00:00").cast("timestamp"))\
                            .alias("LastProcessedChange_DateTime"),
                        F.from_utc_timestamp(df_currency.DataLakeModified_DateTime, 'CST').alias("DataLakeModified_DateTime"),
                        F.trim(df_currency.Country).alias("Country"),
                        F.trim(df_currency.CurrencyName).alias("CurrencyName"),
                        df_currency.RecordId.alias("CurrencyRecordId")
                    )\
                    .withColumn("UpdatedDateTime", F.current_timestamp())\
                    .withColumn("CurrencyHashKey", F.xxhash64("CurrencyRecordId"))

                       

display(dimCurrency_df)

# COMMAND ----------

# DBTITLE 1,write to UC-Silver Schema
#reuse the function shared librabries
saveToDeltaToCatalog(dimCurrency_df, "dev_catalog", "silver", "dimcurrency") 
