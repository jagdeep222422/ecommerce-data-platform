# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🔗 ADLS Gen2 Mount Setup & Unity Catalog Connection
# MAGIC **Author:** Jagdeep (jagdeepa1047@gmail.com)  
# MAGIC **Repository:** https://github.com/jagdeep222422/ecommerce-data-platform
# MAGIC 
# MAGIC This notebook demonstrates how to connect Databricks to Azure Data Lake Storage Gen2 (ADLS Gen2) using:
# MAGIC 1. **Option A (Recommended & Secure):** Databricks Secret Scopes + Azure Service Principal
# MAGIC 2. **Option B (Direct Storage Key):** For quick development / testing

# COMMAND ----------
# Option A: Mount ADLS Gen2 using Databricks Secrets & Service Principal

# 1. Configuration parameters
storage_account_name = dbutils.secrets.get(scope="azure-scope", key="storage-account-name")
container_name       = "ecommerce"
client_id            = dbutils.secrets.get(scope="azure-scope", key="client-id")
tenant_id            = dbutils.secrets.get(scope="azure-scope", key="tenant-id")
client_secret        = dbutils.secrets.get(scope="azure-scope", key="client-secret")

mount_point = "/mnt/datalake"

# 2. Spark OAuth Configuration
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# 3. Mount if not already mounted
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = mount_point,
        extra_configs = configs
    )
    print(f"✅ Successfully mounted {mount_point} to ADLS Gen2!")
else:
    print(f"ℹ️ {mount_point} is already mounted.")

# COMMAND ----------
# Verify access to mounted Bronze, Silver, Gold folders
display(dbutils.fs.ls("/mnt/datalake"))
