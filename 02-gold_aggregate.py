# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

silverTable = spark.table(f"delta.`{config['database_path']}"+"/silver_table`")

# COMMAND ----------

# add CDF to final table
# add expectations library for demo 

# drive towards usage of 

# COMMAND ----------

['AppVersion','AppRoleName', 'clientIP', 'ClientType', 'ClientCity', 'Name','OperationId','ParentId','UserId','SessionId', '_BilledSize']


# COMMAND ----------

display(silverTable)

# COMMAND ----------


