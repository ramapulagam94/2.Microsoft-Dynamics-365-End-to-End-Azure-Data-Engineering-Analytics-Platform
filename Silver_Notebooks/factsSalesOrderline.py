# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "factsalesorderline"

# COMMAND ----------

salesorderlinedf= spark.table("dev_catalog.bronze.salesorderline")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.salesorderline

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  CREATE OR REPLACE TEMP VIEW vwPromotable
# MAGIC  AS
# MAGIC
# MAGIC  SELECT 
# MAGIC    PromotionId,
# MAGIC    CASE PromotionName
# MAGIC      WHEN 'Volume Discount 11 to 20' THEN 11
# MAGIC      WHEN 'Volume Discount 21 to 40' THEN 21
# MAGIC      WHEN 'Volume Discount 41 to 60' THEN 41
# MAGIC      WHEN 'Volume Discount > 60' THEN 61
# MAGIC      ELSE NULL
# MAGIC    END VolumeStart,
# MAGIC    CASE PromotionName
# MAGIC      WHEN 'Volume Discount 11 to 20' THEN 20
# MAGIC      WHEN 'Volume Discount 21 to 40' THEN 40
# MAGIC      WHEN 'Volume Discount 41 to 60' THEN 60
# MAGIC      WHEN 'Volume Discount > 60' THEN 9999999
# MAGIC      ELSE NULL
# MAGIC    END VolumeEnd,
# MAGIC    ValidFrom,
# MAGIC    ValidTo,
# MAGIC    PromoPercentage
# MAGIC
# MAGIC  FROM dev_catalog.bronze.promotable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vwPromotable

# COMMAND ----------

# MAGIC  %sql
# MAGIC
# MAGIC  CREATE OR REPLACE TEMP VIEW vwFactSalesOrderLine
# MAGIC  AS
# MAGIC
# MAGIC  SELECT 
# MAGIC    S.SalesOrderNumber,
# MAGIC    S.SalesOrderLine,
# MAGIC    CASE  
# MAGIC      WHEN isnull(S.LastProcessedChange_DateTime) 
# MAGIC        THEN'1900-01-01'
# MAGIC      ELSE
# MAGIC        S.LastProcessedChange_DateTime 
# MAGIC    END AS LastProcessedChange_DateTime,
# MAGIC    from_utc_timestamp(S.DataLakeModified_DateTime,'CST') AS DataLakeModified_DateTime,
# MAGIC    S.ItemId,
# MAGIC    S.Qty,
# MAGIC    S.Price,
# MAGIC    S.Qty * S.Price AS TotalAmount,
# MAGIC    CASE 
# MAGIC      WHEN PR.PromotionId IS NULL THEN TotalAmount
# MAGIC    ELSE
# MAGIC      TotalAmount * (1- PR.promoPercentage)
# MAGIC    END  AS TotalAmountWithDiscount,  
# MAGIC    S.VatPercentage,
# MAGIC    TotalAmountWithDiscount * s.VatPercentage as VatAmount,
# MAGIC    TotalAmountWithDiscount + VatAmount AS TotalOrderAmount,
# MAGIC    C.CurrencyId,
# MAGIC    from_utc_timestamp(S.BookDate,'CST') AS BookDate,
# MAGIC    cast(date_format(S.BOOKDate,'yyyyMMdd') AS INT ) AS BookDateKey,
# MAGIC    from_utc_timestamp(S.ShippedDate,'CST') AS ShippedDate,
# MAGIC    cast(date_format(S.ShippedDate,'yyyyMMdd') AS INT ) AS ShippedDateKey,
# MAGIC    from_utc_timestamp(S.DeliveredDate,'CST') AS DeliveredDate,
# MAGIC    cast(date_format(S.DeliveredDate,'yyyyMMdd') AS INT ) AS DeliveredKey,
# MAGIC    S.TrackingNumber,
# MAGIC    S.CustId,
# MAGIC    P.PaymentTypeId,
# MAGIC    PR.PromotionId,
# MAGIC    current_timestamp() AS UpdatedDateTime,
# MAGIC    xxhash64(s.RecordId) AS SalesOrderLineRecordId
# MAGIC   FROM dev_catalog.bronze.salesorderline AS S
# MAGIC   LEFT JOIN dev_catalog.bronze.currency AS C ON S.CurrencyCode = C.Code
# MAGIC   LEFT JOIN dev_catalog.silver.dimpaymenttypes AS P  ON S.PaymentTypeDesc = P.PaymentTypeDesc
# MAGIC   LEFT JOIN vwPromotable AS PR ON 
# MAGIC      CASE 
# MAGIC        WHEN month(S.BookDate) = 1 THEN S.BookDate BETWEEN PR.ValidFrom AND PR.ValidTo
# MAGIC      ELSE
# MAGIC        s.Qty BETWEEN PR.VolumeStart AND PR.VolumeEnd  
# MAGIC      END   

# COMMAND ----------

factsalesorderlinedf =  spark.table("vwFactSalesOrderLine")
display(factsalesorderlinedf)

# COMMAND ----------

# DBTITLE 1,final df
df_final = factsalesorderlinedf

# COMMAND ----------

# DBTITLE 1,write to silver schema
saveToDeltaToCatalog(df_final,"dev_catalog","silver",Entity)