# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC /*
# MAGIC created storage credential: using Maganged Identity
# MAGIC created external location: using storage credential (abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net)
# MAGIC
# MAGIC Now, connection established from Databricks to ADLS:
# MAGIC
# MAGIC "In Unity Catalog, Managed Identity is the Azure identity used by Databricks to authenticate with ADLS Gen2. A Storage Credential registers this identity inside Unity Catalog. An External Location maps that credential to a specific ADLS path and controls access. When users access data, Unity Catalog validates permissions and uses the managed identity through the storage credential to securely access ADLS without storing secrets."
# MAGIC
# MAGIC ADLS Gen2 = Bank locker where your data is stored
# MAGIC Databricks = Person who wants to access the locker
# MAGIC Managed Identity = Person's ID card
# MAGIC Storage Credential = Registration of that ID card in Databricks
# MAGIC External Location = Permission saying which locker/path this person can access
# MAGIC
# MAGIC managed identity (for authentication)
# MAGIC         |
# MAGIC         |
# MAGIC Unity Catalog Storage Credential ("When accessing ADLS, use this identity.")
# MAGIC         |
# MAGIC         |
# MAGIC External Location (Using this identity, allow access to this ADLS folder.")
# MAGIC         |
# MAGIC         |
# MAGIC ADLS Gen2
# MAGIC */

# COMMAND ----------

# DBTITLE 1,able to connect Databricks -ADLS
# MAGIC %sql
# MAGIC LIST 'abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net/'; 
# MAGIC
# MAGIC --shall show list of folders/files inside the ADLS-container

# COMMAND ----------

# MAGIC %md
# MAGIC ### No headers for the table when read
# MAGIC  as the dynamics 365 doesnt provide with tables, it gives through manifest and entity cdm.json

# COMMAND ----------

# DBTITLE 1,read some data to confirm
df = spark.read.format("csv")\
    .option("inferSchema", "true")\
    .load("abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net/Purchase/Parties")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### installed librabry: cdm connector in the cluster(workes nodes)----> not working with latest spark releases compute.

# COMMAND ----------

df1 = (spark.read.format("com.microsoft.cdm")
                .option("storage","90111storageadls.dfs.core.windows.net")
                .option("manifestPath","oaon-sandbox-operations-dynamic365/Purchase/Purchase.manifest.cdm.json")
                .option("entity","Parties")
                .load()
)

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC CDM Connector JAR
# MAGIC         |
# MAGIC         | expects
# MAGIC         v
# MAGIC Spark sql.sources.v2.ReadSupport  ❌
# MAGIC
# MAGIC Databricks Runtime 14.3
# MAGIC         |
# MAGIC         | has newer API
# MAGIC         v
# MAGIC Spark DataSource V2  ✅
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC # Manuall create dataframes using data and schema manually

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read manifest file

# COMMAND ----------

manifest_path = "abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net/Purchase/Purchase.manifest.cdm.json"

manifest_df = spark.read.text(manifest_path)

manifest_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,But normally you read it using Python JSON:
import json

manifest = json.loads(
    dbutils.fs.head(manifest_path)
)

print(manifest)
print(f"\n the data type of this file is :{type(manifest)}")

# 1. dbutils.fs.head(manifest_path) reads the file and returns it as a string
# 2. json.loads()->takes that JSON string and converts it into a Python dictionary.

# COMMAND ----------

# DBTITLE 1,now you can easily access the elements in the dictionary
print(manifest["manifestName"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Get entities from manifest

# COMMAND ----------

entities = manifest["entities"]

for entity in entities:
    print(entity["entityName"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test ends here