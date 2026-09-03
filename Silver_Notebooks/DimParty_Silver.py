# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
df_parties = spark.read.table("dev_catalog.bronze.parties")
df_partyaddress = spark.read.table("dev_catalog.bronze.partyaddress")

#we get schema/data types of the above tables

# COMMAND ----------

# DBTITLE 1,Build Dimension table
# F--> import pyspark.sql.functions

#join the tables: parties and partyaddress
dimParty_df = df_parties.join(
                        df_partyaddress, df_parties.PartyId == df_partyaddress.PartyNumber, 'left'
                        ).filter(df_parties.RecordId.isNotNull()
                        ).select(
                            df_parties.PartyId,
                            trim(df_parties.PartyName).alias("PartyName"),
                            #coalesce is used to replace null values with default value
                            F.coalesce(
                                df_parties.LastProcessedChange_DateTime, F.lit("1900-01-01 00:00:00").cast("timestamp"))\
                            .alias("PartiesLastProcessedChange_DateTime"),

                            F.from_utc_timestamp(df_parties.DataLakeModified_DateTime, 'CST').alias("PartiesDataLakeModified_DateTime"),
                            df_parties.PartyAddressCode,
                            F.from_utc_timestamp(df_parties.EstablishedDate,'CST').alias("EstablishedDate"),
                            F.trim(df_parties.PartyEmailId).alias("PartyEmailId"),
                            F.trim(df_parties.PartyContactNumber).alias("PartyContactNumber"),
                            df_parties.RecordId.alias("PartyRecordId"),
                            F.trim(df_parties.TaxId).alias("TaxId"),
                            df_partyaddress.PartyNumber,                    
                            #coalesce is used to replace null values with default value
                            F.coalesce(
                                df_partyaddress.LastProcessedChange_DateTime, F.lit("1900-01-01 00:00:00").cast("timestamp"))\
                            .alias("PartyAddressLastProcessedChange_DateTime"),

                            df_partyaddress.DataLakeModified_DateTime.alias("PartyAddressDataLakeModified_DateTime"),
                            F.trim(df_partyaddress.Address).alias("Address"),
                            F.trim(df_partyaddress.City).alias("City"),
                            F.trim(df_partyaddress.State).alias("State"),
                            F.trim(df_partyaddress.Country).alias("Country"),
                            F.trim(df_partyaddress.ZipCode).alias("ZipCode"),
                            F.trim(df_partyaddress.Region).alias("Region"),
                            F.from_utc_timestamp(df_partyaddress.ValidFrom, 'CST').alias("ValidFrom"),
                            #replace Nulls using when-otheriwse
                            F.when(df_partyaddress.ValidTo.isNull(),'1900-01-01 00:00:00')\
                            .otherwise(df_partyaddress.ValidTo)\
                            .cast("timestamp")\
                            .alias("ValidTo"),
                            df_partyaddress.RecordId.alias("PartyAddressRecordId")
                        #add column for current timestamp
                         )\
            .withColumn("UpdatedDateTime", F.current_timestamp())\
            .withColumn("PartyHashKey", F.xxhash64("PartyRecordId")) #Add hash key to dimParty_df dataframe(not input dataframes)
                

display(dimParty_df)

# COMMAND ----------

# DBTITLE 1,write to UC-Silver Schema
#reuse the function shared librabries

saveToDeltaToCatalog(dimParty_df, "dev_catalog", "silver", "dimParty") 

# this functions creates silver schema if not exists and add dimParty is the new tableName that will written to silver layer.