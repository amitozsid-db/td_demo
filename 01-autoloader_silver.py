# Databricks notebook source
# MAGIC %run ./00-setup

# COMMAND ----------

# MAGIC %run ./00-qpl_listener_pyspark

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException


# COMMAND ----------

land_data()

# COMMAND ----------

stream = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("maxFilesPerTrigger", 1)
          .option("header", "true")
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # rescue ( stream will not fail ), failOnNewColumns, none ( ignore and do not fail)
          .option("cloudFiles.schemaLocation", config['main_directory']+'/stream_schema')
          .option("cloudFiles.schemaHints", "Properties MAP<STRING,STRING>, _BilledSize INT")
          .load(config['source_directory'])
          .select(F.col("*"), F.col("_metadata"), F.col("_metadata").alias('row_created_metadata'))
         )

# COMMAND ----------

def  write_first_time(df, target_location):
  
  print('first time write triggered', file=sys.stderr)
  df.drop('rowid').write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f"{config['database']}.{target_location.split('/')[-1]}")

#Upsert Logic
def upsert_data(target_table, changesDF, epocId):

  print('Upsert triggered', file=sys.stderr)
  
  changesDF.persist() 
  #get the set of columns to update
  updateCols = {}
  for column in changesDF.columns:
    if column not in ["OperationId", "row_created_metadata"]:
      updateCols.update({column: f"s.{column}"})

# Filter updates to the most recent updates for each ID.
  partWin = Window.partitionBy('OperationId').orderBy(F.col('time').desc())
  updates = (changesDF.withColumn('row_id', F.row_number().over(partWin)).where(F.col('row_id')==1).drop('row_id'))
  
# Inner join approach
# keyMaxDate = changesDF.select('OperationId', F.col('time').alias('update_time')).groupby('OperationId').agg(max('update_time').alias('update_time'))
# updates = (changesDF.alias('changesDF')
#            .join(keyMaxDate.alias('keyMaxDate'), [changesDF.id == keyMaxDate.id , changesDF._metadata.file_modification_time == keyMaxDate.update_time], 'inner').select('changesDF.*'))  
  try:  
    output_table = DeltaTable.forPath(spark, target_table)                                                                                        
    (output_table
     .alias("t")
     .merge(
        updates.alias("s"), 
        "t.OperationId = s.OperationId and s.time > t.time")
  #    .whenMatchedDelete(condition = '')
     .whenMatchedUpdate(set = updateCols)
     .whenNotMatchedInsertAll()
     .execute())
  except AnalysisException as e:
    print(f"upsert failed triggering full overwrite {e}", file=sys.stderr)
    write_first_time(updates, target_table)
  
  changesDF.unpersist()
  
  
#create Audit Table
def audit_data(audit_table, df, epochId):
    #audit logic
    #create_time, discovery_time, commit_time and archive_time
    audit = df.select(F.col("_metadata.file_name"), F.col("_metadata.file_modification_time").alias("file_creation_time"))  \
      .withColumn("file_commit_time", F.lit(datetime.datetime.now())) \
      .withColumn('ingest_id', F.lit(epochId)).distinct()
    audit.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .save(audit_table)

#foreachbatch function
def batch_data(target_table, audit_table, df, epochId):
  #merge then audit
  upsert_data(target_table, df, epochId)
  audit_data(audit_table, df, epochId)

# COMMAND ----------

(stream
 .observe("metric", #observe api for custom metric logging, This is added to the QPL
          F.count(F.lit(1)).alias("cnt"), 
          F.count(F.col("_rescued_data")).alias("malformed"))
 .writeStream
 .queryName('demoIngestionStream')
 .format("delta")
 .outputMode("update")
 .option("checkpointLocation", config['main_directory']+'/stream_checkpoint')
 .option("mergeSchema", "true")
#  .trigger(processingTime='5 seconds')
 .trigger(availableNow=True)
 .foreachBatch(lambda batch_df, batch_id: batch_data(config['database_path']+'/silver_table', config['database_path']+'/audit_table', batch_df, batch_id))
 .start())

# COMMAND ----------

spark.table(f"delta.`{config['database_path']}"+"/silver_table`").display()
