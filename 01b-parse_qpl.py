# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

import pyspark.sql.functions as F
import json
import re

# COMMAND ----------

# get_key_val = F.udf(lambda x: json.loads(x), 'MAP<STRING,STRING>' )

@udf('MAP<STRING,STRING>' )
def get_key_val(x):
  try:
    return json.loads(x)
  except:
    return None
  
@udf('MAP<STRING,INTEGER>' )  
def get_trigger_ms(string):
  try:
    lst = string.replace('{','').replace('}','').split(',')
    obj = dict([(a.split('=')[0].strip(),int(a.split('=')[1])) for a in lst])
    return (obj)
  except:
    return None
  
@udf('INTEGER')
def get_outstanding(string,exp):
  try:
    return int(re.findall(exp,s)[0].split('=')[1])
  except:
    return None

# COMMAND ----------

raw_logs = spark.read.format('json').load(config['qps_log_directory']).select('*','_metadata')

create_columns = ['numInputRows','inputRowsPerSecond','batchId','processedRowsPerSecond']
create_batch_duration_ms = ['triggerExecution','queryPlanning','gatBatch','commitOffsets','latestOffset','addBatch','walCommit']


select_cols = ['stream_id','run_id','stream_name','status','input_time','event_time','batchId','numInputRows','inputRowsPerSecond','processedRowsPerSecond',
                'outstanding_files','triggerExecution','queryPlanning','gatBatch','commitOffsets','latestOffset','addBatch','walCommit','termination_exception','SOURCE_FILE','PARSED_BATCH_INFO']

parsed_logs = (raw_logs
               .withColumn('PARSED_BATCH_INFO', get_key_val(F.col('BATCH_INFO')))
               .withColumn('PARSED_BATCH_INFO', get_key_val(F.col('BATCH_INFO')))
               .withColumn('BATCH_DURATION_MS', F.col('PARSED_BATCH_INFO')['durationMs'])
               .withColumn('BATCH_DURATION_MS', get_trigger_ms(F.col('BATCH_DURATION_MS')))
               .withColumn('OUTSTANDING_FILES', get_outstanding(F.col('PARSED_BATCH_INFO')['sources'], F.lit('numFilesOutstanding=\d+')))
               .withColumn('STREAM_ID', F.coalesce(F.col('STREAM_ID'), F.col('PARSED_BATCH_INFO').id))
               .withColumn('RUN_ID', F.coalesce(F.col('RUN_ID'), F.col('PARSED_BATCH_INFO').runId))
               .withColumn('STREAM_NAME',  F.coalesce(F.col('STREAM_NAME'), F.col('PARSED_BATCH_INFO')['name']))
               .withColumn('INPUT_TIME',F.col('EVENT_TIME').cast('timestamp'))
               .withColumn('EVENT_TIME',F.col('PARSED_BATCH_INFO').timestamp)
               .withColumn('SOURCE_FILE', F.col('_metadata').file_path)
              )

for key in create_columns:
  parsed_logs = parsed_logs.withColumn(key, F.col('PARSED_BATCH_INFO')[key])
  
for key in create_batch_duration_ms:
  parsed_logs = parsed_logs.withColumn(key, F.col('BATCH_DURATION_MS')[key])

cast_to_int = ['OUTSTANDING_FILES','numInputRows','inputRowsPerSecond','batchId','processedRowsPerSecond']

for key in cast_to_int:
  parsed_logs = parsed_logs.withColumn(key, F.col(key).cast('int'))

parsed_logs = parsed_logs.select(select_cols)

# COMMAND ----------

parsed_logs.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f"{config['database']}.qpl_parsed")

# COMMAND ----------

display(parsed_logs)
