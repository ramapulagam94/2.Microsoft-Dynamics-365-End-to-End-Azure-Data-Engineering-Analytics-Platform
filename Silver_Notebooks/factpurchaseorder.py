# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "factpurchaseorder"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

purchaseorderDf= spark.table("dev_catalog.bronze.purchaseorder")
dimcostcenterDf= spark.table("dev_catalog.silver.dimcostcenter")
dimcurrencyDf= spark.table("dev_catalog.silver.dimcurrency")

# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

factpurchaseorderDf = purchaseorderDf.filter(purchaseorderDf.RecordId.isNotNull()
    ).join(dimcostcenterDf, purchaseorderDf.CostCenter == dimcostcenterDf.CostCenterNumber, "left"
    ).join(dimcurrencyDf, purchaseorderDf.currencycode == dimcurrencyDf.CurrencyCode, "left"
    ).select(
        purchaseorderDf.PoNumber,
        purchaseorderDf.LineItem,
        purchaseorderDf.VendId.alias("VendorKey"),
        F.when(purchaseorderDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseorderDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(purchaseorderDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        purchaseorderDf.Qty,
        purchaseorderDf.PurchasePrice,
        purchaseorderDf.TotalOrder,
        purchaseorderDf.CostCenter.alias("CostCenterKey"),
        dimcostcenterDf.Vat.alias("VatAmount"),
        F.round((purchaseorderDf.TotalOrder + (purchaseorderDf.TotalOrder * dimcostcenterDf.Vat)),4).alias("TotalAmount"),
        purchaseorderDf.ExchangeRate,
        purchaseorderDf.Itemkey,
        dimcurrencyDf.CurrencyId.alias("CurrencyKey"),
        F.from_utc_timestamp(purchaseorderDf.OrderDate,'CST').alias("OrderDate"),
        F.from_utc_timestamp(purchaseorderDf.ShipDate,'CST').alias("ShipDate"),
        F.from_utc_timestamp(purchaseorderDf.DeliveredDate,'CST').alias("DeliveredDate"),

        #formatted date to int mainly because DateKey is being used as a numeric key
        #in traditional star-schema/data-warehouse design,
        #--> YYYYMMDD as an integer is commonly used because it's compact, easy to join/filter, and clearly represents the date.

        F.date_format(purchaseorderDf.OrderDate,'yyyyMMdd').cast("int").alias("OrderDateKey"),
        F.date_format(purchaseorderDf.ShipDate,'yyyyMMdd').cast("int").alias("ShipDateKey"),
        F.date_format(purchaseorderDf.DeliveredDate,'yyyyMMdd').cast("int").alias("DeliveredDateKey"),
        purchaseorderDf.TrackingNumber,
        purchaseorderDf.Batchid.alias("BatchId"),
        purchaseorderDf.CreatedBy,
        purchaseorderDf.RecordId.alias("PurchaseOrderRecordId"),
        purchaseorderDf.CategoryId.alias("CategoryKey")
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PurchaseOrderHashKey", F.xxhash64("PurchaseOrderRecordId")
    )
display(factpurchaseorderDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = factpurchaseorderDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

saveToDeltaToCatalog(df_final,"dev_catalog","silver",Entity)

# COMMAND ----------

