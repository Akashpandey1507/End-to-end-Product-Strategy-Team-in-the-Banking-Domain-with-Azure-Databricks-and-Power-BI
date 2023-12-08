# Databricks notebook source
# MAGIC %run "/Repos/iamakash.pandey@outlook.com/End-to-end-Product-Strategy-Team-in-the-Banking-Domain-with-Azure-Databricks-and-Power-BI/databricks_notebook/data_preparation"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create the secrets

# COMMAND ----------

database = dbutils.secrets.get(scope="myscope", key="database")
sqlpass = dbutils.secrets.get(scope="myscope", key="sqlpass")
sqlserver = dbutils.secrets.get(scope="myscope", key="sqlserver")
sqluser = dbutils.secrets.get(scope="myscope", key="sqluser")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Show the data

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # data store on Azure SQL database

# COMMAND ----------

server = sqlserver
database = database
username = sqluser
password = sqlpass
#driver= '{ODBC Driver 17 for SQL Server}'
#connection = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};Driver={driver}')

# COMMAND ----------

df.write \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{server};databaseName={database}") \
  .option("dbtable", "fact_spends") \
  .option("user", username) \
  .option("password", password) \
  .mode("overwrite") \
  .save()
