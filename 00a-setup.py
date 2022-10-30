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
config['expectation_suit_directory'] = f'/dbfs/tmp/{username_sql_compatible}/fs_demo/expectation_suit'

# COMMAND ----------

def land_data(alter_schema:bool =False )-> None:
  try:
    seek_lines = [random.randint(0,500) for i in range(0,11)]
    time_now = datetime.datetime.now()
    file_name = re.subn(r'[:,.,\s,-]','_',str(time_now))[0]
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

# COMMAND ----------

# import json

# base_string = {"time": "2022-07-05T18:00:29.1282604Z","resourceId": "/SUBSCRIPTIONS/2DCF4535-5D3F-4516-9AAE-B27709B54CE8/RESOURCEGROUPS/RGP-TDDEV-DE-CI-CAP-01/PROVIDERS/MICROSOFT.INSIGHTS/COMPONENTS/AIS-TDDEV-DE-CI-CAP-01","ResourceGUID": "b57e5d4e-ab32-42e4-a152-ef360ef71160","Type": "AppEvents","AppRoleInstance": "LP-97YH463","AppRoleName": "TDL.Chatbot.Skill._1mg","AppVersion": "1.0.0.0","ClientIP": "0.0.0.0","ClientType": "PC","IKey": "d5b44f19-c820-4d3b-ba45-58272777379e","_BilledSize": 1080,"OperationName": "POST Bot/Post","OperationId": "","ParentId": "b135ba6a78613744","SDKVersion": "dotnetc:2.20.0-103","SessionId": "7RdX9ITLwEr0Ow5o5yFcSQiwRnE7hGqhjbY6El9K96g=","UserId": "emulator1ad45fa6-75ab-47cd-8026-1770cc1281ed","Properties": {"activityId": "5a2ca8e0-fc8c-11ec-98cc-cb61fdfea37c","DeveloperMode": "true","activityType": "message","replyActivityId": "5a2ca8e0-fc8c-11ec-98cc-cb61fdfea37c","channelId": "emulator","recipientId": "1ad45fa6-75ab-47cd-8026-1770cc1281ed","AspNetCoreEnvironment": "dev","conversationId": "2ceec7f1-fc8c-11ec-a073-2160285ba2e4|livechat","locale": "en-US","CoreVersion": "1.9.5.4","recipientName": "User"},"Name": "BotMessageSend","ItemCount": 1}

# writefilehandler = open(f"/dbfs{config['source_directory']}/big_file_ingest_test.json",'a')
# for i in range(0,3000000):
#   base_string['OperationId'] = i + 3000000
#   string = json.dumps(base_string)
#   writefilehandler.writelines(f"{string}\n")
# writefilehandler.close()

# COMMAND ----------

#%sh 
#du -sh /dbfs/tmp/amitoz_sidhu/fs_demo/message_source/big_file_ingest_test.json

# COMMAND ----------

#%sh
#wc -l /dbfs/tmp/amitoz_sidhu/fs_demo/message_source/big_file_ingest_test.json
