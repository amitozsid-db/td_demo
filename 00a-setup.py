# Databricks notebook source
#imports
import sys
import itertools
import random
import datetime
import re

# COMMAND ----------

if 'config' not in locals():
  config = {}
  
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username_sql_compatible = useremail.split('@')[0].replace(".", "_")


# COMMAND ----------

config['useremail'] = useremail
config['database'] = f'{username_sql_compatible}_fs_demo'
config['main_directory'] = f'/tmp/{username_sql_compatible}/fs_demo'
config['database_path'] = f'/tmp/{username_sql_compatible}/fs_demo/table_write_path'
config['source_directory'] = f'/tmp/{username_sql_compatible}/fs_demo/message_source'
config['qps_log_directory'] = f'/tmp/{username_sql_compatible}/fs_demo/qps_log'

# COMMAND ----------

def land_data(alter_schema:bool =False )-> None:
  try:
    seek_lines = [random.randint(0,500) for i in range(0,11)]
    time_now = datetime.datetime.now()
    file_name = re.subn('[:,.,\s,-]','_',str(time_now))[0]
    hour,min,sec = time_now.hour, time_now.minute, time_now.second
    
    readfilehandler = open('./ingestion_messages.json','rb',0)
    all_lines = readfilehandler.readlines()
    lines_write = [all_lines[i] for i in seek_lines]

    write_path = f"{config['source_directory']}/{hour}/{min}/{sec}"
    dbutils.fs.mkdirs(write_path)
    writefilehandler = open(f"/dbfs{config['source_directory']}/{hour}/{min}/{sec}/{file_name}.json",'wb')
    writefilehandler.writelines(lines_write)
    
    readfilehandler.close()
    writefilehandler.close()
    
  except Exception as e: 
    print(f"failed to load data to source directory: {e}", sys.stderr)
    
  return None

# COMMAND ----------

def clean_setup():
  dbutils.fs.rm(config['main_directory'],True)
  spark.sql(f"DROP DATABASE IF EXISTS {config['database']} CASCADE ")

  return None

# COMMAND ----------

dbutils.fs.mkdirs(config['source_directory'])
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['database']} LOCATION '{config['database_path']}'")
