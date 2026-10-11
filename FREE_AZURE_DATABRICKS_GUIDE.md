# 🆓 100% Free Setup Guide: Azure & Databricks for Your Project

**Author:** Jagdeep ([jagdeepa1047@gmail.com](mailto:jagdeepa1047@gmail.com))  
**Repository:** [https://github.com/jagdeep222422/ecommerce-data-platform](https://github.com/jagdeep222422/ecommerce-data-platform)

---

## 🎯 Which Free Option Should You Choose?

| Option | Cost | Need Credit Card? | Best For |
|---|---|---|---|
| **Option 1: Databricks Community Edition (100% Free Forever)** | **$0 / ₹0** | ❌ **No credit card required** | Quickest demo, practicing PySpark notebooks, MLflow, and SQL |
| **Option 2: Azure Free Account + Azure Databricks ($200 Credit)** | **$0 for 30 days** | ✅ Needs card (no charges) | Real Azure Data Lake (ADLS Gen2) mounting, Unity Catalog, and full enterprise DLT |

---

# 🚀 OPTION 1: Databricks Community Edition (100% Free Forever, No Credit Card)

This is the easiest way to run your code live inside Databricks in under 5 minutes without paying a single rupee.

### Step 1: Create Free Databricks Account
1. Go to: **[https://community.cloud.databricks.com/login.html](https://community.cloud.databricks.com/login.html)**
2. Click **"Sign Up"** (or go to `databricks.com/try-databricks` and make sure you select **"Get started with Community Edition"** at the bottom).
3. Fill in your name (**Jagdeep**), email (**`jagdeepa1047@gmail.com`**), and verify your email.

### Step 2: Create a Free Cluster
1. In your Databricks Community dashboard, click **Compute** on the left sidebar.
2. Click **Create Compute**.
3. Name: `demo-cluster`
4. Databricks Runtime: Choose **13.3 LTS (Scala 2.12, Spark 3.4.1)**.
5. Click **Create Cluster** (Community gives you 15GB micro-cluster free).

### Step 3: Import Your Project Notebooks
1. Go to **Workspace** $\to$ **Users** $\to$ `jagdeepa1047@gmail.com`.
2. Right-click and choose **Import** $\to$ **URL** (or Git Integration).
3. Paste: `https://github.com/jagdeep222422/ecommerce-data-platform`
4. Open the notebook `notebooks/00_mount_azure_adls.py` or run `dlt/pipeline.py`.
5. Attach the cluster and click **Run All**!

---

# ☁️ OPTION 2: Real Azure Account ($200 / ₹15,800 Free Credits for 30 Days)

Use this if you want to show recruiters real **Azure Portal $\to$ ADLS Gen2 $\to$ Azure Databricks** screenshots.

### Step 1: Sign up for Azure Free Tier
1. Go to: **[https://azure.microsoft.com/free/](https://azure.microsoft.com/free/)**
2. Click **Start free**.
3. Sign in with your Microsoft account (or create one using `jagdeepa1047@gmail.com`).
4. Enter phone number and payment card for identity check (Azure charges a small ₹2 refundable verification and does **not** charge you after).
5. You receive **$200 USD (approx. ₹15,800 INR)** credit valid for 30 days + 12 months of free services.

---

### Step 2: Create Azure Data Lake Storage Gen2 (ADLS Gen2)
1. In the search bar at the top of Azure Portal, type **Storage accounts** and click **+ Create**.
2. **Basics tab:**
   - Resource Group: Click *Create new* $\to$ name it `rg-ecommerce-demo`.
   - Storage account name: e.g. `adlsjagdeepdemo` (must be lowercase and numbers only).
   - Region: Select nearest (e.g. *Central India* or *East US*).
   - Performance: *Standard*.
3. **Advanced tab (CRITICAL STEP):**
   - Check the box: **"Enable hierarchical namespace"** (This turns regular Blob storage into **Data Lake Storage Gen2**!).
4. Click **Review + Create** $\to$ **Create**.
5. Once deployed:
   - Click **Go to resource**.
   - Under **Data storage**, click **Containers** $\to$ click **+ Container**.
   - Name: `ecommerce` $\to$ Access level: *Private* $\to$ Click **Create**.
   - Inside `ecommerce`, click **+ Add Directory** and create 3 folders:
     - `bronze`
     - `silver`
     - `gold`

---

### Step 3: Create Azure Databricks Workspace (14-Day Free Trial)
1. In the Azure Portal search bar, type **Azure Databricks** $\to$ click **+ Create**.
2. **Basics:**
   - Resource group: `rg-ecommerce-demo`.
   - Workspace name: `databricks-ecommerce-jagdeep`.
   - Region: Same region as storage.
   - Pricing Tier: Select **Trial (Premium - 14-Days Free DBUs)** or **Standard**.
3. Click **Review + Create** $\to$ **Create**.
4. Once deployed, click **Go to resource** $\to$ Click **Launch Workspace**.

---

### Step 4: Get Storage Access Key (Simplest Free Method)
To connect Databricks to your new ADLS Gen2 container:
1. In Azure Portal, go back to your Storage Account (`adlsjagdeepdemo`).
2. On the left menu under **Security + networking**, click **Access keys**.
3. Click **Show** next to `key1` and copy the **Key** string.

---

### Step 5: Mount ADLS Gen2 in Databricks (Copy & Paste Code)
1. In your Azure Databricks workspace, click **+ New** $\to$ **Notebook**.
2. Paste this simple 4-line mounting code:

```python
# Replace with your storage account name and the copied key1
STORAGE_ACCOUNT = "adlsjagdeepdemo"
CONTAINER_NAME = "ecommerce"
ACCOUNT_KEY = "<PASTE_COPIED_KEY1_HERE>"

MOUNT_POINT = "/mnt/datalake"

# Setup configuration
configs = {
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net": ACCOUNT_KEY
}

# Mount ADLS Gen2
if not any(mount.mountPoint == MOUNT_POINT for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
        mount_point = MOUNT_POINT,
        extra_configs = configs
    )
    print("✅ ADLS Gen2 mounted successfully at /mnt/datalake!")
else:
    print("ℹ️ Already mounted at /mnt/datalake!")
```

3. Run the cell (`Shift + Enter`).
4. Verify by running:
```python
display(dbutils.fs.ls("/mnt/datalake"))
```
You will see your `bronze/`, `silver/`, and `gold/` folders directly in Databricks!

---

## 💡 How to Keep Azure $0 Cost (Important Safety Tips):
1. **Auto-termination:** In Databricks Compute settings, set **"Terminate after 20 minutes of inactivity"**.
2. **Single-node cluster:** Choose **Single Node** cluster (Driver type: `Standard_DS3_v2`) to consume minimal compute.
3. When you are done taking screenshots for LinkedIn/interviews, click **Delete** on the resource group `rg-ecommerce-demo` if you don't need it.
