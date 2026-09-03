# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,List of secrets scope
dbutils.secrets.listScopes()

# COMMAND ----------

# DBTITLE 1,List of Secrets in scope
dbutils.secrets.list("kv-secrets")

# COMMAND ----------

# DBTITLE 1,get the credentials from secret scope
clientID = dbutils.secrets.get(scope = "kv-secrets", key = "ClientID")
clientSecret = dbutils.secrets.get(scope = "kv-secrets", key = "ClientSecret")
tenantID = dbutils.secrets.get(scope = "kv-secrets", key = "TenantID")

print(clientID)
print(clientSecret)
print(tenantID)

# output: [REDACTED] means the actual value has been intentionally hidden (masked) for security reasons.

# COMMAND ----------

# DBTITLE 1,Legacy method-service principal
# MAGIC %skip
# MAGIC #legacy direct OAuth authentication method: way to authenticate Azure Databricks to ADLS Gen2 using a Service Principal (Client ID + Client Secret + Tenant ID).
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.90111storageadls.dfs.core.windows.net","OAuth")
# MAGIC #---> "Use OAuth authentication when accessing this storage account"
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.90111storageadls.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.
# MAGIC                ClientCredsTokenProvider")
# MAGIC #---> "Get the ADLS token using Service Principal credentials"
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.90111storageadls.dfs.core.windows.net",clientID)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.90111storageadls.dfs.core.windows.net",clientSecret)
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.90111storageadls.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenantID}/oauth2/token")
# MAGIC #--->is where Databricks requests the OAuth token.

# COMMAND ----------

# DBTITLE 1,Recommended Approach
# MAGIC %sql
# MAGIC /*
# MAGIC Recommended approach: IS unity catalog approach:
# MAGIC
# MAGIC Service Principal
# MAGIC         |
# MAGIC         |
# MAGIC Unity Catalog Storage Credential
# MAGIC         |
# MAGIC         |
# MAGIC External Location
# MAGIC         |
# MAGIC         |
# MAGIC ADLS Gen2
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ### I have already authenticated with managed identity

# COMMAND ----------

# MAGIC %sql SHOW STORAGE CREDENTIALS

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL storage_credential;