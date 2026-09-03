# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

dev_catalog.bronze.promotabledev_catalog.bronze.promotable

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimvertical"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

workerdf= spark.table("dev_catalog.bronze.workertable")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.silver.dimvertical (
# MAGIC   VerticalId BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Vertical STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

#For DISTINCT, Spark groups all NULL occurrences together and returns one NULL value.


df = workerdf.select(F.expr("trim(Vertical) AS Vertical")).distinct()
display(df)

# COMMAND ----------

verticaldf = spark.table("dev_catalog.silver.dimvertical")
display(verticaldf)

# COMMAND ----------

newrowsdf=df.filter(F.col("Vertical").isNotNull()).exceptAll(verticaldf.select("Vertical"))
display(newrowsdf)

# COMMAND ----------

spark.sql("insert into dev_catalog.silver.dimvertical(vertical) select  Vertical from {newrowsdf}",newrowsdf=newrowsdf)

# COMMAND ----------

display(spark.table("dev_catalog.silver.dimvertical"))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev_catalog.bronze.workertable(Vertical)VALUES("Data & AI")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM dev_catalog.bronze.workertable WHERE Vertical ="Data & AI"

# COMMAND ----------

