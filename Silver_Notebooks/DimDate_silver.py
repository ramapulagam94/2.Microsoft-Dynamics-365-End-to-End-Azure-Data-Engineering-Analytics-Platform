# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/ramapulagam3@gmail.com/ProjecRepoForDatabrciks/Shared_librabries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimdate"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

df_fiscalperiod = spark.table("dev_catalog.bronze.fiscalperiod")

# COMMAND ----------

display(df_fiscalperiod)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Date Dimension from Python

# COMMAND ----------

start_date = datetime.date(2018,1,1)
end_date = start_date + dateutil.relativedelta.relativedelta(years=8,month=12,day=31)


start_date = datetime.datetime.strptime(
    f"{start_date}", "%Y-%m-%d"
)
end_date = datetime.datetime.strptime(
    f"{end_date}", "%Y-%m-%d"
)
print(start_date)
print(end_date)

# COMMAND ----------

# DBTITLE 1,Pandas is much useful bringing dates here

#Creates dates from start_date to end_date., D-means daily
#date_range() initially produces a Pandas DatetimeIndex, convert to pandas df using .to_frame(name="Date")

datepddf = pd.date_range(start_date,end_date, freq='D').to_frame(name='Date') 
datedf=spark.createDataFrame(datepddf) #Convert Pandas → Spark
display(datedf)

# COMMAND ----------

# DBTITLE 1,This is a PySpark range join.
#1. filter() = remove unwanted rows BEFORE the join
#2. Join condition = decide WHICH rows match: 
#-->Find the fiscal period where the date is greater than or equal to the fiscal start date AND less than or equal to the fiscal end date.

# Date          Fiscal Period
#----------    -------------
#03-29-2026    FY2025-26
#03-30-2026    FY2025-26
#--------------------------------------------------------------
#datedf.join(                              # LEFT table
#    df_fiscalperiod.filter(...),          # RIGHT table
#    condition,                            # HOW to match (condition)
#    "left"                                # JOIN TYPE
#)

joindf = (
    datedf.join(
        df_fiscalperiod.filter(df_fiscalperiod.RecordId.isNotNull()),
         (datedf.Date >= df_fiscalperiod.FiscalStartDate) & (datedf.Date <= df_fiscalperiod.FiscalEndDate),
        "left",
    ))
display(joindf)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC              RIGHT TABLE
# MAGIC         df_fiscalperiod
# MAGIC                │
# MAGIC                │
# MAGIC           filter()
# MAGIC                │
# MAGIC                ▼
# MAGIC      Only RecordId NOT NULL
# MAGIC                │
# MAGIC                ▼
# MAGIC           JOIN happens
# MAGIC                │
# MAGIC                │
# MAGIC        JOIN CONDITION
# MAGIC                │
# MAGIC       ┌────────┴─────────┐
# MAGIC       │                  │
# MAGIC  Date >= FiscalStart   Date <= FiscalEnd
# MAGIC       │                  │
# MAGIC       └────────┬─────────┘
# MAGIC                │
# MAGIC                ▼
# MAGIC            LEFT JOIN
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ####Build Date Dimension

# COMMAND ----------

datedimdf = joindf.select(
    "Date",
    F.date_format(F.col("Date"), "yyyyMMdd").cast("int").alias("DateId"),
    F.year(F.col("Date")).alias("Year"),
    F.month(F.col("Date")).alias("Month"),
    F.date_format(F.col("Date"), "MMM").cast("string").alias("MonthName"),
    F.dayofmonth(F.col("Date")).alias("Day"),
    F.date_format(F.col("Date"), "E").cast("string").alias("DayName"), #"E" means short day name.
    F.quarter(F.col("Date")).alias("Quarter"),
    F.col("FiscalPeriodName").alias("FiscalPeriodName"),    
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    F.concat(F.lit("FY"),"FiscalYear").alias("FiscalYear"),
    F.lit(UpdatedDateTime).alias("UpdatedDateTime"),
    F.xxhash64("DateId").alias("DateKey")
)
display(datedimdf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = datedimdf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

saveToDeltaToCatalog(df_final,"dev_catalog","silver", Entity)