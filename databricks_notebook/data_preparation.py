# Databricks notebook source
# MAGIC %run "/Repos/iamakash.pandey@outlook.com/End-to-end-Product-Strategy-Team-in-the-Banking-Domain-with-Azure-Databricks-and-Power-BI/databricks_notebook/data_ingest_from_Azure_blob_storage_and_SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Work on df (main datasets)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.select([sum(col(column).isNull().cast('int')).alias(column) for column in df.columns]))

# COMMAND ----------

df.columns

# COMMAND ----------

len(df.columns)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Now, will work on customer (df1)

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df1.select([sum(col(column).isNull().cast('int')).alias(column) for column in df1.columns]))

# COMMAND ----------

df1.columns

# COMMAND ----------

len(df1.columns)

# COMMAND ----------

df1.count()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1 = df1.withColumn(
                "avg_income", col('avg_income').cast('int')
)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()
