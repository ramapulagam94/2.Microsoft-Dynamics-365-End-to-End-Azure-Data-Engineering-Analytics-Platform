# Microsoft-Dynamics-365-End-to-End-Azure-Data-Engineering-Analytics-Platform
Building a governed, quality-driven data pipeline using ADLS Gen2, Azure Databricks, Unity Catalog, Medallion Architecture, Dimensional Modeling and Power BI.

<br>

### 📋 1. Project Overview

- Developed an **end-to-end Azure data engineering pipeline** to ingest and process data from **Microsoft Dynamics 365**.
- Used **Azure Data Lake Storage Gen2 (ADLS Gen2)** as the centralized storage layer for raw and processed data.
- Implemented **Azure Databricks notebooks** using **PySpark and Spark SQL** for data cleansing, transformation, validation, and business-rule processing.
- Followed a **Medallion Architecture (Raw →Bronze → Silver)** to progressively refine the data.
- Used **Databricks Unity Catalog** for data governance, access control, and management of external tables.
- Created **Fact and Dimension tables** to provide curated analytical datasets for downstream **Power BI reporting and analytics**.

#### Note: 
In this implementation, I did not create a separate **Gold layer**. After applying cleansing, validation, and business transformations in Silver, I created the required Fact and Dimension tables there and used them for Power BI consumption. 

A separate Gold/serving layer could be introduced in a larger production implementation where additional aggregation, semantic modeling, or multiple downstream consumers require it.

<br>

### 🎯 2. Business Problem
OAON COMM(Order Anything Online commerce) generates a large amount of data from Microsoft Dynamics 365 related to
  purchasing, sales, customers, vendors, and products.
</p>

<ul>
  <li>The data is mainly used for <strong>day-to-day business transactions</strong> and is not directly suitable for reporting.</li>
  <li>Data from different business areas needs to be <strong>brought together</strong> to get a complete business view.</li>
  <li>Data may contain <strong>missing, duplicate, or inconsistent records</strong>, which can affect report accuracy.</li>
  <li>Business users need reliable insights into <strong>sales, purchases, vendor performance, products, and customer activity</strong>.</li>
  <li>As the data volume increases, it becomes difficult to <strong>process and analyze the data efficiently</strong>.</li>
  <li>Business teams need <strong>consistent and trustworthy data</strong> for reporting and decision-making.</li>
</ul>


<br>

### 🛠️ 3. Technology Stack
<img width="650" alt="image" src="https://github.com/user-attachments/assets/f5e5bc8c-2908-4c6a-bc40-d66caf06c9c4" />

<br>

<br>

### 🏗️ 4. Project Architecture
<img width="1400" alt="image" src="https://github.com/user-attachments/assets/b936b218-d536-42c5-aa2e-240d8c4a57d3" />
<br>

<br>

<img width="700" alt="image" src="https://github.com/user-attachments/assets/354f088c-9872-4f0a-82ad-1fd08c2d1978" />

<br>

### 🔄 5. End-to-End Data Flow
<img width="1536" height="1024" alt="ChatGPT Image Sep 2, 2026, 06_14_41 PM" src="https://github.com/user-attachments/assets/b61626c3-1a5a-4670-acaf-75be2893c1b9" />

<br>

### ⚙️ 6. Implementation
#### 6.1📥 Source Data:
-----------------------------------
The source system is Microsoft Dynamics 365, providing business data across:

- Sales
- urchase
- HR

The data is provided in Common Data Model (CDM) format along with manifest and metadata files. The manifest defines entities, attributes, data types, file locations, and other schema-related information.
<br>

#### 6.2💾 ADLS Gen2 – Raw Layer
-----------------------------------
The Raw layer is used as the initial landing zone and preserves the source data without business transformations.

Key activities:

- Store source CSV files.
- Store CDM manifest and metadata files.
- Preserve original source data.
- Maintain the source folder structure.
- Provide a reliable input layer for Databricks processing.

Flow:

Dynamics 365
     ↓
CDM Data + Manifest
     ↓
ADLS Gen2 – Raw
<br>

#### 6.3🔷 Databricks & Unity Catalog
-----------------------------------
-> Azure Databricks is used as the main data processing platform.
-> PySpark and Spark SQL are used for ingestion, transformation, validation, and data modeling.
-> Azure Managed Identity is used for secure access to ADLS Gen2, avoiding the need to store storage credentials directly in notebooks.

Unity Catalog is used for:

- Catalog and schema management
- Table management
- Access control
- Data governance
- Centralized metadata management

The main catalog structure is:

dev_catalog
│
├── bronze
│
└── silver
<br>

#### 6.4🥉 Bronze Layer
-----------------------------------
The Bronze layer contains source-aligned Delta tables created from the raw Dynamics 365 data. The CDM manifest and metadata are used to understand the source schema and apply the appropriate data types.

Key activities:

- Read raw source files from ADLS Gen2.
- Read CDM manifest and metadata.
- Apply source schema and data types.
- Perform basic validation.
- Convert source data into Delta format.
- Register and manage Delta tables through Unity Catalog.

Bronze tables include: costcenter, currency, custtable etc.
<br>

#### 6.5🥈 Silver Layer
-----------------------------------
The Silver layer converts source-aligned Bronze data into cleaned and business-ready data.

Data cleansing:

- Handle NULL and missing values.
- Remove duplicate records.
- Standardize data formats.
- Perform data type conversions.
- Apply date and timestamp transformations.

Business transformations:

- Join related Dynamics 365 entities.
- Apply business rules.
- Derive business columns.
- Create hash keys where required.
- Filter invalid records.
- Prepare data for analytical modeling.

The Silver layer contains the final curated data used for reporting.
<br>

#### 6.6✅ Data Quality
-----------------------------------
Data-quality validation is performed as part of the Silver processing before the final analytical tables are consumed.

Key checks include:

- Record-count validation
- Duplicate checks
- NULL checks
- Primary/business key validation
- Referential integrity checks
- Invalid-record checks

This ensures that only validated and business-ready data is used for the dimensional model and Power BI reporting.
<br>

#### 6.7⭐ Dimensional Modeling
-----------------------------------
The curated Silver data is organized into a dimensional model consisting of Fact and Dimension tables.

Dimension Tables
dimcostcenter
dimcurrency
dimcusttable
dimdate
dimparty
dimpaymenttypes
dimpromotable
dimpurchasecategory
dimpurchitem
dimvendor
dimvertical
dimworker
Fact Tables
factpurchaseorder
factsalesorderline

The dimensional model simplifies analytical queries and allows Power BI to establish relationships between business entities and transactional facts.
<br>

#### 6.8🚀 Optimization
------------------------
Spark and Delta Lake optimization techniques are applied to improve pipeline performance and scalability.

Key considerations include:

- Appropriate partitioning of data.
- Avoiding unnecessary data shuffles.
- Selecting appropriate join strategies.
- Using broadcast joins where appropriate for small datasets.
- Reducing unnecessary transformations and actions.
- Using Delta Lake for reliable storage and efficient processing.
- Processing only the required columns and data.
- Reusing intermediate data where caching provides a performance benefit.

The objective is to reduce processing time, resource consumption, and unnecessary Spark operations.
<br>

#### 6.9📊 Power BI
---------------------
Power BI consumes the curated Silver Fact and Dimension tables.

Key activities:

- Connect Power BI to the curated analytical tables.
- Establish relationships between Fact and Dimension tables.
- Build the semantic model.
- Create measures and KPIs.
- Develop reports and dashboards.
- Provide business users with analytical insights.

Final flow:

Silver Fact + Dimension Tables
              ↓
         Power BI
              ↓
    Semantic Model
              ↓
       KPIs & Reports
              ↓
        Dashboards
<br>

#### 6.10🔐 Governance
The solution uses Unity Catalog for centralized governance and table management.

Key governance considerations include:

- Centralized metadata management.
- Role-based access control.
- Secure access to ADLS through Managed Identity.
- Controlled access to catalog and schemas.
- Delta tables for reliable and consistent data management.
<br>

### 🧠 7. Key Engineering Decisions
### ⚠️ 8. Challenges & Solutions
### 📈 9. Project Outcomes
<br>

### 🔮 10. Future Enhancements
The platform can be extended with:

1. Introduce a dedicated Gold schema for reporting-ready
   dimensional tables.
2. Implement automated Databricks Workflows orchestration.
3. Add incremental processing instead of full loads.
4. Implement SCD Type 2 where historical tracking is required.
5. Add automated data quality framework.
6. Implement CI/CD using Azure DevOps or GitHub Actions.
7. Add monitoring and alerting.
8. 8. Implement parameterized notebooks.
9. Add automated unit/integration testing.
10. Optimize Delta tables using OPTIMIZE and appropriate
    partitioning/Z-Ordering where applicable.

