# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
import datetime, pytz
import pandas as pd
from delta.tables import DeltaTable
import json
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def generate_suffix(provided_start_point=None, provided_end_point=None):
  """function to generate dates which will be used to generate the files to be read"""
  
  start_point = (get_last_end_point() if provided_start_point is None else 
                 datetime.datetime.strptime(provided_start_point,'%Y-%m-%d:%H').replace(tzinfo=pytz.timezone("Asia/Kolkata")))
  
  end_point = ((datetime.datetime.now(pytz.timezone("Asia/Kolkata")) - datetime.timedelta(hours=1)) if provided_end_point is None else 
               datetime.datetime.strptime(provided_end_point,'%Y-%m-%d:%H').replace(tzinfo=pytz.timezone("Asia/Kolkata")))
 
  assert start_point < end_point, f"start point: {start_point} is larger than end point: {end_point}, kindly check provided/calculated values"
  generated_suffix_range = pd.date_range(start_point,end_point,freq='h').strftime('y=%Y/m=%m/d=%d/h=%H/m=00').tolist()[1:]

  return generated_suffix_range



def get_last_end_point():
  """ function to lookup last logged last porcessed hour. """
  
  logging_path = config['database_path']+'/batch/audit_table'
  last_logged_point = None
  try: 
    last_logged_point = (spark.read.format('delta').load(logging_path).select(F.max('source_folder')).rdd.flatMap(lambda x:x).collect()[0])
    last_logged_point = datetime.datetime.strptime(last_logged_point,'%Y-%m-%d:%H').replace(tzinfo=pytz.timezone("Asia/Kolkata"))
  except AnalysisException as e:
    return datetime.datetime.now(pytz.timezone("Asia/Kolkata")) - datetime.timedelta(hours=2)

  return last_logged_point

@udf()
def get_source_path(x):
  val = ''.join(x.split('/')[-6:-1]).replace('y=','').replace('m=','').replace('d=','').replace('h=','')
  return f"{val[:4]}-{val[4:6]}-{val[6:8]}:{val[8:10]}"

@udf()
def get_partition_path(x):
  return ''.join(x.split('/')[-6:-1]).replace('y=','').replace('m=','').replace('d=','').replace('h=','')

@udf('MAP<STRING,STRING>')
def get_key_val(x):
  try:
    return json.loads(x)
  except:
    return None
  
@udf('MAP<STRING,STRING>' )  
def convert_map(string):
  try:
    lst = string.replace('{','').replace('}','').split(',')
    obj = dict([(a.split('=')[0].strip(),(a.split('=')[1])) for a in lst])
    return (obj)
  except:
    return None

# COMMAND ----------

suffix_list = generate_suffix("2022-11-10:02","2022-11-10:04")
# suffix_list = generate_suffix()

files_to_read = [f"{config['source_directory']}/batch/{suffix}" for suffix in suffix_list]
files_to_read
# spark.conf.set('spark.sql.files.ignoreMissingFiles', 'true')

spark.conf.set('spark.databricks.delta.properties.defaults.autoOptimize.autoOptimizeWrite', 'true')

# COMMAND ----------

df = (spark.read.format('text').load(files_to_read).distinct()
      .select('*','_metadata')
     )

parsed = (df.withColumn('parsed_json',get_key_val('value'))
          .withColumn('generated_md5_indx',F.md5('value'))
          .select('value','generated_md5_indx','_metadata', F.explode('parsed_json').alias('key','exp_value'))
          .groupBy('generated_md5_indx', '_metadata').pivot('key').agg(F.first('exp_value'))
          .withColumn('properties', convert_map('properties'))
          .withColumn('Measurements', convert_map('Measurements'))
          .withColumn('source_file_folder', get_partition_path(F.col('_metadata.file_path')))
         )

# COMMAND ----------

def  write_first_time(df, target_location):
  """
  partition if necessary
  """
  (df.drop('_metadata').write.format('delta').mode('overwrite')
   .option('overwriteSchema','true').option('path', target_location)
   .save())
  return None

def write_data_frame(updates, target_table):
  """ 
  partition if necessary
  """
  try:  
    output_table = DeltaTable.forPath(spark, target_table)                                                                                        
    (output_table
     .alias("t")
     .merge(
        updates.alias("s"), 
        "t.generated_md5_indx = s.generated_md5_indx  and t.source_file_folder = s.source_file_folder")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
  except AnalysisException as e:
    if e.getErrorClass() == 'DELTA_MISSING_DELTA_TABLE':
      write_first_time(updates, target_table)
    else:
       print(f"Merge failed {e}", file=sys.stderr)
        
    return None    
        

def update_processed_log(df, audit_table)-> None:
  """function to keep track of the files that were last processed. This will be used to identify the start for the next read"""
  
  audit_write_log = (df.select('_metadata.file_path','_metadata.file_name','_metadata.file_size', '_metadata.file_modification_time')
                     .withColumn('source_folder', get_source_path(F.col('file_path'))).withColumn('log_time',F.current_timestamp())
                     .distinct())
               
  audit_write_log.write.format("delta").mode("append").option("mergeSchema", "true").save(audit_table)
  
  return None

# COMMAND ----------

write_data_frame(parsed, config['database_path']+'/batch/bronze')
update_processed_log(df.select('_metadata'),  config['database_path']+'/batch/audit_table')

# COMMAND ----------

dbutils.fs.ls(config['database_path']+'/batch/bronze')

# COMMAND ----------

spark.sql(f"""select * from delta.`{config['database_path']}/batch/bronze`""").display()

# COMMAND ----------

spark.sql(f"""select * from delta.`{config['database_path']}/batch/audit_table`""").display()

# COMMAND ----------

spark.sql(f"""describe history delta.`{config['database_path']}/batch/bronze`""").display()

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
