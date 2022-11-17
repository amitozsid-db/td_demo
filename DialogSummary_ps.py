# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

## Import Libraries
import pandas as pd 
from datetime import date,datetime
from datetime import timedelta
import pytz

# COMMAND ----------

df = spark.table(f"delta.`{config['main_directory']}/silver_jump`")

# COMMAND ----------

a

# COMMAND ----------

def get_conversation_details(mode, df,start_date=None, end_date=None):
    
    final_output = pd.DataFrame()
    
    IST = pytz.timezone('Asia/Kolkata')
    
    if mode.lower() == 'yesterday':
        yesterday = datetime.now(IST).date() - timedelta(days = 1)
        dates = [datetime.strftime(yesterday, '%Y-%m-%d')]
    else:
        dates = pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
        dates = [datetime.strftime(date_, '%Y-%m-%d') for date_ in dates]
        
    bandoned_dialogs = df.where(((F.col('AbandonedUserLeftDialog')=='True')|(F.col('AbandonedUserCancelledDialog')=='True'))&(F.col('AgentDialog')=='False'))
    failed_dialogs = df.where(F.col('FailedDialog')=='True')
    agent_escalations = df.where((F.col('AgentDialog')=='True') & (F.col('AbandonedUserCancelledDialog')=='False'))
    open_agent_connection = df.where((F.col('AgentDialog')=='True')& (F.col('AbandonedUserCancelledDialog')=='False')& (F.col('AbandonedUserLeftDialog')=='True'))

    open_agent_connection = df.where((F.col('AgentDialog')=='True')& (F.col('AbandonedUserCancelledDialog')=='False')& (F.col('AbandonedUserLeftDialog')=='False'))
    
    
    

# COMMAND ----------

start_date = date(2022,11,10)
end_date = date(2022,11,11)

output = get_conversation_details('',df, start_date, end_date)


# output = get_conversation_details('yesterday')

