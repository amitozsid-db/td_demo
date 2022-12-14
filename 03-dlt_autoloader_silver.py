# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window
import datetime

# COMMAND ----------

username_sql_compatible = 'amitoz_sidhu'

# COMMAND ----------

@dlt.create_table(comment="New raw loan data incrementally ingested from cloud object storage landing zone")
def dlt_ingestion_table():
  
  raw = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("header", "true")
          .option("cloudFiles.schemaHints", "Properties MAP<STRING,STRING>, _BilledSize INT")
          .load(f"dbfs:/tmp/{username_sql_compatible}/fs_demo/message_source/")
          .select(F.col("*"), F.col("_metadata"), F.col("_metadata").alias('row_created_metadata')))

  
  return raw

# COMMAND ----------

@dlt.create_table(comment="Record FileName Read")
def dlt_audit_table():
  raw = dlt.readStream('dlt_ingestion_table')
  audit_row = (raw.select(F.col("_metadata.file_name"), F.col("_metadata.file_modification_time").alias("file_creation_time"))
               .withColumn("file_commit_time", F.lit(datetime.datetime.now()))
               )
  
  return audit_row

# COMMAND ----------

@dlt.view
def upsert_source():
  raw = dlt.readStream('dlt_ingestion_table') 
  partWin = Window.partitionBy('OperationId').orderBy(F.col('time').desc())
  updates = (raw.withColumn('row_id', F.row_number().over(partWin)).where(F.col('row_id')==1).drop('row_id'))
  
  return raw

# COMMAND ----------

dlt.create_streaming_live_table(
  name = "dlt_silver",
  comment = "upsert applied table",
  table_properties={"pipelines.autoOptimize.zOrderCols": "time"}
)

dlt.apply_changes(
  target = "dlt_silver",
  source = "upsert_source",
  keys = ["OperationId"],
  sequence_by = F.col("time"),
  stored_as_scd_type = 1
)
  

# COMMAND ----------

select_cols = ['time','AppRoleInstance','AppRoleName','AppVersion','ClientType','Type','clientBrowser', 'clientIP',  'ClientCity', 'Name','OperationId','OperationName','ParentId','UserId','SessionId', '_BilledSize','ItemCount','properties.recipientName', 'properties.locale','properties.activityType','properties.conversationId']

# COMMAND ----------


@dlt.expect_or_drop("ClientType null check ", "ClientType IS NOT NULL")
@dlt.create_table(name='dlt_client_type_distribution')
def dlt_client_type_distribution():
  base_df = (spark.table('LIVE.dlt_silver')
               .select(select_cols)
               .withColumn('date',F.to_date(F.col('time'))))
  
  total_users = base_df.groupBy('date').agg(F.countDistinct('UserId').alias('totalUsers'))

  client_type_distribution = (base_df
                            .groupBy('date','ClientType')
                            .agg(F.countDistinct('UserId').alias('userBase'))
                            .join(total_users ,'date','left' )
                            .withColumn('percentUsersCap', (F.col('userBase')/F.col('totalUsers'))*100))
  
  return client_type_distribution
  

# COMMAND ----------

@dlt.create_table(name='dlt_user_prefered_channel')
def dlt_user_prefered_channel():
  base_df = (spark.table('LIVE.dlt_silver')
               .select(select_cols)
               .withColumn('date',F.to_date(F.col('time'))))
  
  user_prefered_channel = (base_df.groupBy('date','UserId',"activityType")
                           .agg(F.sum('ItemCount').alias('interactions'), F.collect_set('AppRoleName').alias('communitcationChannel')))
  
  return user_prefered_channel
