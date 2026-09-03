# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Data ingestion pipeline
# MAGIC ELT: 
# MAGIC - Extracting data from ADLS,
# MAGIC - Applying minimal transformation (schema application)-> considered as data ingestion only
# MAGIC - Loading it into Delta,
# MAGIC
# MAGIC | Step | What you're doing                     | Common name                                |
# MAGIC | ---- | ------------------------------------- | ------------------------------------------ |
# MAGIC | 1    | Read `Purchase.manifest.cdm.json`     | Metadata discovery                         |
# MAGIC | 2    | Read each `.cdm.json`                 | Schema inference / Schema extraction       |
# MAGIC | 3    | Build `StructType`                    | Dynamic schema generation                  |
# MAGIC | 4    | Read CSV files into a Spark DataFrame | Data ingestion                             |
# MAGIC | 5    | Write as Delta to ADLS                | Data loading                               |
# MAGIC | 6    | Register in Unity Catalog             | Table registration / Metadata registration |
# MAGIC
# MAGIC
# MAGIC "I built a metadata-driven ingestion pipeline that reads Dynamics 365 CDM manifests, dynamically constructs Spark schemas from the entity metadata, ingests the CSV data into Spark DataFrames, writes it as Delta format to ADLS Gen2, and registers the resulting Delta tables in Unity Catalog for governed access."
# MAGIC
# MAGIC Metadata-driven ingestion framework (because the manifest and .cdm.json files drive the process dynamically).

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Dynamics 365 Export
# MAGIC         │
# MAGIC         ▼
# MAGIC ADLS Gen2 (CSV + CDM metadata)
# MAGIC         │
# MAGIC         ▼
# MAGIC Read Manifest
# MAGIC         │
# MAGIC         ▼
# MAGIC Read Entity Schema (.cdm.json)
# MAGIC         │
# MAGIC         ▼
# MAGIC Build Spark Schema
# MAGIC         │
# MAGIC         ▼
# MAGIC Read CSV into DataFrame
# MAGIC         │
# MAGIC         ▼
# MAGIC Write Delta to ADLS
# MAGIC         │
# MAGIC         ▼
# MAGIC Register Table in Unity Catalog
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps
# MAGIC - Read the manifest (*.manifest.cdm.json).
# MAGIC - Read each entity definition (*.cdm.json).
# MAGIC - Build the Spark schema using your build_schema() function.
# MAGIC - Read the corresponding CSV files using spark.read.csv().
# MAGIC - Write the data as Delta tables to your external ADLS location (or register them in Unity Catalog).

# COMMAND ----------

# DBTITLE 1,Import data types
import json

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType
)

# COMMAND ----------

# DBTITLE 1,Mapping CDM types to Spark
type_mapping = {
    "String": StringType(),
    "Int32": IntegerType(),
    "Int64": LongType(),
    "Double": DoubleType(),
    "Decimal": DoubleType(),
    "Boolean": BooleanType(),
    "Date": DateType(),
    "DateTime": TimestampType(),
    "Guid": StringType()
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-1

# COMMAND ----------

# DBTITLE 1,Read manifest.cdm.json
import json

base_path = "abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net"

folders = ["Purchase", "Hr", "Sales", "Others"] 

for folder in folders:
    manifest_path = f"{base_path}/{folder}/{folder}.manifest.cdm.json"

    print(f"\n========== {folder} ==========")

    manifest = json.loads(dbutils.fs.head(manifest_path, 10000000))
    entities = manifest["entities"]

    print("Entities Found:")
    for entity in entities:
        print(entity["entityName"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2

# COMMAND ----------

# DBTITLE 1,Function to create Spark schema
def build_schema(cdm_json):

    schema = StructType()
    entity = cdm_json["definitions"][0]

    for col in entity["hasAttributes"]:
        spark_type = type_mapping.get(col.get("dataFormat", "String"),StringType())

        schema.add(StructField(col["name"],spark_type,True))

    return schema

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Purchase.manifest.cdm.json
# MAGIC         │
# MAGIC         ▼
# MAGIC Get entity names
# MAGIC         │
# MAGIC         ▼
# MAGIC Vendor.cdm.json ─────────► build_schema() ───────┐
# MAGIC                                                   │
# MAGIC Vendor/ (CSV files) ──► spark.read.csv(schema) ───┤
# MAGIC                                                   ▼
# MAGIC                                         Write Delta Table
# MAGIC                                                   │
# MAGIC                                                   ▼
# MAGIC                          External ADLS + Unity Catalog Table
# MAGIC
# MAGIC Repeat for every entity...
# MAGIC Repeat for HR...
# MAGIC Repeat for Sales...
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS Gen2->In case of .csv not in folders, 
# MAGIC create new folders and move .csv using dbutils.fs commands

# COMMAND ----------

# DBTITLE 1,just moving .csv files into their respective folders in ADLS
# Sales folder has .csvfiles and manifest file in one folder.

source_path = "abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net/Sales"

files = dbutils.fs.ls(source_path)

for file in files:

    # Skip directories
    if file.isDir():
        continue

    # Process only CSV files
    if file.name.lower().endswith(".csv"):

        folder_name = file.name[:-4]   # Remove .csv

        folder_path = f"{source_path}/{folder_name}"

        # Create folder if it doesn't exist
        dbutils.fs.mkdirs(folder_path)

        # Move CSV into the folder
        dbutils.fs.mv(file.path, f"{folder_path}/{file.name}")

        print(f"Moved {file.name} to {folder_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-3

# COMMAND ----------

# DBTITLE 1,Read data into dataframe and write to external location
import json

base_path = "abfss://oaon-sandbox-operations-dynamic365@90111storageadls.dfs.core.windows.net"

folders = ["Purchase", "Hr", "Sales",  "Others"]

# Initialize once
success = []
failed = []

for folder in folders:

    try:
        # Read manifest
        manifest_path = f"{base_path}/{folder}/{folder}.manifest.cdm.json"
        manifest = json.loads(dbutils.fs.head(manifest_path, 10000000))

        print(f"\n========== Processing Folder: {folder} ==========")

    except Exception as e:
        print(f"Unable to read manifest for {folder}")
        print(e)
        failed.append((folder, "Manifest Read", str(e)))
        continue

    for entity in manifest["entities"]:

        entity_name = entity["entityName"]

        try:
            print(f"\nProcessing {entity_name}")

            # Read entity definition
            cdm_path = f"{base_path}/{folder}/{entity_name}.cdm.json"
            cdm_json = json.loads(dbutils.fs.head(cdm_path, 10000000))

            # Build schema
            schema = build_schema(cdm_json)

            # Read CSV
            data_path = f"{base_path}/{folder}/{entity_name}"

            df = (spark.read
                    .option("header", True)
                    .schema(schema)
                    .csv(data_path))

            # Write Delta
            output_path = f"{base_path}/raw/{folder}/{entity_name}"

            (df.write
                .format("delta")
                .mode("overwrite")
                .save(output_path))

            print(f"✓ {entity_name} loaded successfully")

            success.append(entity_name)

        except Exception as e:
            print(f"✗ Failed : {entity_name}")
            print(f"Reason : {e}")

            failed.append((folder, entity_name, str(e)))

            # Continue with next entity
            continue

print("\n================ SUMMARY ================")

print(f"\nSuccessful Tables ({len(success)})")
for table in success:
    print(table)

print(f"\nFailed Tables ({len(failed)})")
for folder, table, error in failed:
    print(f"{folder}/{table}")
    print(f"Error : {error}")
    print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### For production, I would make this framework more robust by:
# MAGIC
# MAGIC - Reading the CSV delimiter, header flag, escape character, and newline from exhibitsTraits instead of hardcoding them.
# MAGIC - Supporting nested folders and incremental exports.
# MAGIC - Handling schema evolution and malformed records.
# MAGIC - Logging row counts and failed entities.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-4: Raw folder in ADLS gen2 is ready

# COMMAND ----------

# DBTITLE 1,Example: Folder strcuture now ADLS Gen2
# MAGIC %sql
# MAGIC /*
# MAGIC raw/
# MAGIC └── Purchase/
# MAGIC     └── PurchaseOrder/
# MAGIC         ├── _delta_log/
# MAGIC         │   ├── 00000000000000000000.json
# MAGIC         │   └── ...
# MAGIC         ├── part-00000-8b5f....snappy.parquet
# MAGIC         ├── part-00001-a91c....snappy.parquet
# MAGIC         └── ...
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-5: You can register external tables In unity catalog