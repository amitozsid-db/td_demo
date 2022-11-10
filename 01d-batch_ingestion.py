# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
import datetime, pytz
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

s = datetime.datetime.now(pytz.timezone("Asia/Kolkata")) - datetime.timedelta(days=5)
e = datetime.datetime.now(pytz.timezone("Asia/Kolkata")) - datetime.timedelta(hours=3)

datetime.datetime.strftime(pd.date_range(s,e,freq='h').tolist()[0],"%Y/%m/%d/%H")

# COMMAND ----------

def get_last_end_point():
  """ function to lookup last logged last porcessed hour. """
  
  logging_path = config['main_directory'] + "/last_read_file"
  win = Widow.orderBy('log_time')
  last_logged_point = None
  try: 
    last_logged_point = (spark.read.format('delta').load(logging_path)
                         .withColumn('indx', F.row_number().over(win))
                         .where(F.col('indx')==1).select('last_read_hour')
                         .rdd.flatMap(lambda x:x).collect()[0])
    
  except AnalysisException as e:
    last_logged_point =  datetime.datetime.now(pytz.timezone("Asia/Kolkata")) - datetime.delta(hours-1)
  finally: 
    return last_logged_point

  return last_logged_point


def update_processed_log(df)-> None:
  """function to keep track of the files that were last processed. This will be used to identify the start for the next read"""
  
  audit = (df.select(F.col("_metadata.file_name"), F.col("_metadata.file_modification_time").alias("file_creation_time"))
               .withColumn("log_time", F.lit(datetime.datetime.now()).withColumn('ingest_id', F.lit(epochId)).distinct()))
               
  audit.write.format("delta").mode("append").option("mergeSchema", "true").save(audit_table)
  
  return None


def generate_suffix(provided_start_point=None, provided_end_point=None):
  """function to generate dates which will be used to generate the files to be read"""
  
  start_point = (get_last_end_point() if provided_start_point is None else 
                 datetime.datetime.strptime(provided_start_point,'%Y-%m-%d:%H').replace(tzinfo=pytz.timezone("Asia/Kolkata")))
  
  end_point = ((datetime.datetime.now(pytz.timezone("Asia/Kolkata"))) if provided_end_point is None else 
               datetime.datetime.strptime(provided_end_point,'%Y-%m-%d:%H').replace(tzinfo=pytz.timezone("Asia/Kolkata")))

  
  assert start_point < end_point, f"start point: {start_point} is larger than end point: {end_point}, kindly check provided/calculated values"
  
  generated_suffix_range = pd.date_range(start_point,end_point,freq='h').strftime('y=%Y/m=%m/d=%d/h=%H/m=00').tolist()[1:]

  return generated_suffix_range

# COMMAND ----------

generate_suffix("2022-11-08:10","2022-11-10:03")

# COMMAND ----------



# COMMAND ----------

# operation, event-time , properties
# what were duplicates

# <uuid>, o1,t1, p1
# o1,t1, p1

# <uuid>, o1,t1, p2

# 10/h8 
# 10/h9 - > h8+h9
# 10/h9 - > o1,t1, p2


# <uuid1>, o1,t1, p2
# <uuid2>, o1,t1, p1


# mongo -> sql ( logic ) -> v1
# -> sql ( logic2 ) -> v2
# mongo 0> db ->sql+mongo ( uuid)

# tab to keep track of older files and check if they need tp be reprocessed based on x logic -> future thing
# keep track of duplicate rows etc
