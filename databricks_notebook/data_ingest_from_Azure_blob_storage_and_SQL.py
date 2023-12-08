# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ------------------------------From Azure Blob Storage-------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create the secrets Scope

# COMMAND ----------

container = dbutils.secrets.get(scope="myscope", key="container")
database = dbutils.secrets.get(scope="myscope", key="database")
key = dbutils.secrets.get(scope="myscope", key="key")
sqlpass = dbutils.secrets.get(scope="myscope", key="sqlpass")
sqlserver = dbutils.secrets.get(scope="myscope", key="sqlserver")
sqluser = dbutils.secrets.get(scope="myscope", key="sqluser")
storageaccount = dbutils.secrets.get(scope="myscope", key="storageaccount")

# COMMAND ----------

# Getting all details from Azure
storage_account_name = storageaccount
storage_account_access_key = key
blob_container = container

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Azure Configure

# COMMAND ----------

spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Fetching the data from Azure blob storage

# COMMAND ----------

filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"
display(dbutils.fs.ls(filePath))

# COMMAND ----------

fact_spends = spark.read.csv(
                            f"wasbs://banking@adlsstoage.blob.core.windows.net/fact_spends.csv",
                            header=True,
                            inferSchema=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ------------------------------From Azure Blob Storage-------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ------------------------------From Azure SQL database------------------------

# COMMAND ----------

server = sqlserver
database = database
username = sqluser
password = sqlpass
#driver= '{ODBC Driver 17 for SQL Server}'
#connection = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};Driver={driver}')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create the link

# COMMAND ----------

# JDBC URL for Azure SQL Database
jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database};user={username};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create the connection of Azure SQL

# COMMAND ----------

# Define connection properties
connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Fetching data from Azure SQL Database

# COMMAND ----------

# Load data into a PySpark DataFrame
dim_customers = spark.read.jdbc(url=jdbc_url, table="dbo.dim_customers", properties=connection_properties)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ------------------------------From Azure SQL database------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ----------------Create the Duplicate of Original Dataframe------------------

# COMMAND ----------

df = fact_spends.alias("copy")
df1 = dim_customers.alias("copy")

# COMMAND ----------

df.display()

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ----------------Create the Duplicate of Original Dataframe------------------
