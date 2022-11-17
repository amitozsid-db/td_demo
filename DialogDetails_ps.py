# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pyspark.sql.functions as F
from collections import OrderedDict


# COMMAND ----------

output_columns_names = [
    "Dialog",
    "ConversationId",
    "start_time",
    "end_time",
    "AbandonedUserLeftDialog",
    "AbandonedUserCancelledDialog",
    "FailedDialog",
    "AgentDialog",
]

# COMMAND ----------

expected_source_data_columns_expr = {
    "time" : "time",
    "Name" : "Name",
    "properties.conversationId" : "Properties_conversationId",
    "properties.MasterBotConversationId" : "Properties_MasterBotConversationId",
    "properties.text" : "Properties_text",
    "properties.activityId" : "Properties_activityId",
    "properties.TopAnswer" : "Properties_TopAnswer",
    "properties.TopAnswerScore" : "Properties_TopAnswerScore",
    "properties.intent" : "Properties_intent",
    "properties.intentScore" : "Properties_intentScore",
    "properties.userUtterance" : "Properties_userUtterance",
    "properties.DialogId" : "Properties_DialogId",
    "properties.StepName" : "Properties_StepName",
    "properties.InstanceId" : "Properties_InstanceId",
    "AppRoleName" : "AppRoleName"
}

# COMMAND ----------

def create_df(data_table, expected_source_data_columns_expr, date_range):
  """
  
  """
    # missing column logic to be added if required
  base_df = (spark.table(data_table)
        .withColumn('date', F.to_date(F.col('time')))
#           .where(F.col('date').isin(date_range)) #  filter for required date range if required
         )
  
  for _as,to in expected_source_data_columns_expr.items():
    base_df = base_df.withColumn(to,F.col(_as))

  final_df =base_df.withColumn("ConversationId",F.coalesce(F.col("Properties_MasterBotConversationId"),F.col("Properties_conversationId")))
  return final_df

# COMMAND ----------

def get_Properties_activityId(df):
    Properties_activityId = df[((df['Name'] == 'BotMessageReceived') & (df['Properties_text'].notnull()))]['Properties_activityId'].values
    return list(OrderedDict.fromkeys(Properties_activityId))
  
def get_Properties_InstanceId(df, Properties_activityId):
  Properties_InstanceId = df[(df['Properties_InstanceId'].notnull()) & (df.Properties_activityId.isin(Properties_activityId))]['Properties_InstanceId'].values
  return list(OrderedDict.fromkeys(Properties_InstanceId))

def get_instanceId_start_index(df, Properties_InstanceId):
    temp = df[(df['Properties_InstanceId'].isin(Properties_InstanceId)) 
              & ((df.Properties_StepName == 'InitialStepAsync') & (df.Properties_DialogId == 'MainDialog.WaterfallDialog')) 
              | ((df.Properties_StepName == 'EstablishChatStepAsync') & (df.Properties_DialogId == 'WaterfallDialog'))]

    index_InitialStepAsync = temp.index.tolist()
    Properties_InstanceId_InitialStepAsync = temp.Properties_InstanceId.tolist()

    return dict(zip(Properties_InstanceId_InitialStepAsync, index_InitialStepAsync))
  
def get_instanceId_end_index(df, Properties_InstanceId):
    temp = df[(df['Properties_InstanceId'].isin(Properties_InstanceId)) & 
              ((df.Properties_DialogId == 'DisambiguateUserUtteranceDialog.WaterfallDialog') 
               | (df.Properties_StepName == 'FinalStepAsync') 
               | (df.Name == 'WaterfallComplete'))]

    index_FinalStepAsync = temp.index.tolist()
    Properties_InstanceId_FinalStepAsync = temp.Properties_InstanceId.tolist()
    
    return dict(zip(Properties_InstanceId_FinalStepAsync, index_FinalStepAsync))
  
def map_instanceId_with_start_and_index(dict_InitialStepAsync, dict_FinalStepAsync):
    dialog_index = []
    
    for InstanceId,index in dict_InitialStepAsync.items():
        if InstanceId in dict_FinalStepAsync:
            dialog_index.append((InstanceId, dict_InitialStepAsync[InstanceId], dict_FinalStepAsync[InstanceId]))
            
    return dialog_index

# COMMAND ----------

def map_dialog_and_AbandonedUserLeftDialog(data, dialog_index, Properties_activityId, dict_InitialStepAsync):
    df = data.copy()
    df['Dialog'] = np.nan
    df['AbandonedUserLeftDialog'] = np.nan
    
    # If there is only one abandoned dialog in the conversation
    if len(dialog_index) == 0 and len(Properties_activityId) == 1 and len(dict_InitialStepAsync) == 1:
        first_InitialStepAsync = list(dict_InitialStepAsync.values())[0]
        df.loc[df.index >= first_InitialStepAsync, 'Dialog'] = 'D1|' + list(dict_InitialStepAsync)[-1]
        df.loc[df.index >= first_InitialStepAsync, 'AbandonedUserLeftDialog'] = True
        
    elif len(dialog_index) == 0:
        list_InitialStepAsync = list(OrderedDict(dict_InitialStepAsync).items())
        
        for i, (InstanceId, start) in enumerate(list_InitialStepAsync):
            if i == len(list_InitialStepAsync)-1:
                df.loc[df.index >= start, 'Dialog'] = 'D' + str(i+1) + '|' + InstanceId 
                df.loc[df.index >= start, 'AbandonedUserLeftDialog'] = True
            else:
                end = list_InitialStepAsync[i+1][1]
                next_InstanceId = list_InitialStepAsync[i+1][0]
                condition = (df.index >= start) & (df.index <= end) & ((df.Properties_InstanceId == InstanceId) | (df.Properties_InstanceId.isnull()))
                df.loc[condition, 'Dialog'] = 'D' + str(i+1) + '|' + InstanceId
                df.loc[condition, 'AbandonedUserLeftDialog'] = True
    
    else:
        for i, (InstanceId, start, end) in enumerate(dialog_index):
            df.loc[(df.index >= start) & (df.index <= end), 'Dialog'] = 'D' + str(i+1) + '|' + InstanceId

        last_InitialStepAsync = list(dict_InitialStepAsync.values())[-1]
        last_dialog = dialog_index[-1][-1]
        
        # To capture last abandoned dialog where user left the chat
        if last_InitialStepAsync > last_dialog:
            df.loc[df.index >= last_InitialStepAsync, 'Dialog'] = 'D' + str(i+2) + '|' + list(dict_InitialStepAsync)[-1]
            df.loc[df.index >= last_InitialStepAsync, 'AbandonedUserLeftDialog'] = True

    return df
  
def get_dialog_group(df):
    dialog_group_df = df[df['Dialog'].notnull()].groupby('Dialog').agg({'Name':lambda x: list(x), 
                                                                        'Properties_intent':lambda x: list(x), 
                                                                        'Properties_StepName':lambda x: list(x), 
                                                                        'AbandonedUserLeftDialog':lambda x: True if list(x)[0] == True else False,
                                                                        'time':lambda x: list((pd.Series(x).min(), pd.Series(x).max())),
                                                                        'ConversationId':lambda x: list(x)[0]})
    dialog_group_df['start_time'] = dialog_group_df['time'].apply(lambda x: x[0])
    dialog_group_df['end_time'] = dialog_group_df['time'].apply(lambda x: x[-1])
    dialog_group_df.drop(['time'], axis=1, inplace=True)
    
    return dialog_group_df
  
def map_dialogs_into_buckets(data):
    dialog_group_df = data.copy()
    dialog_group_df['AbandonedUserCancelledDialog'] = dialog_group_df.Properties_intent.apply(lambda x: 'Cancel' in x)
    dialog_group_df['FailedDialog'] = dialog_group_df.Name.apply(lambda x: 'FailedUtterance' in x)
    dialog_group_df['AgentDialog'] = dialog_group_df.Properties_StepName.apply(lambda x: 'EstablishChatStepAsync' in x)
    return dialog_group_df
  
def get_final_df(df, dialog_group_df, all_dialogs_from_db):
    final_df = pd.merge(df, dialog_group_df[['AbandonedUserCancelledDialog', 'FailedDialog', 'AgentDialog', 'start_time', 'end_time']], on='Dialog', how='left')

    final_df = final_df[final_df['Dialog'].notnull()].groupby('Dialog').agg({'ConversationId':lambda x: list(x)[0],
                                                                             'start_time':lambda x: list(x)[0],
                                                                             'end_time':lambda x: list(x)[0],
                                                                             'AbandonedUserLeftDialog':lambda x: True if list(x)[0] == True else False,
                                                                             'AbandonedUserCancelledDialog':lambda x: True if list(x)[0] == True else False,
                                                                             'FailedDialog':lambda x: True if list(x)[0] == True else False,
                                                                             'AgentDialog':lambda x: True if list(x)[0] == True else False})
    final_df = final_df.reset_index(level=0)
    final_df = final_df[output_columns_names]
    final_df = final_df[~final_df.Dialog.isin(all_dialogs_from_db)]
    final_df.reset_index(drop=True, inplace=True)
    
    return final_df

# COMMAND ----------


def apply_business_logic(conv_id_df: pd.DataFrame) -> pd.DataFrame:
  """
  """
  all_dialogs_from_db = []
  Properties_activityId = get_Properties_activityId(conv_id_df)
  Properties_InstanceId = get_Properties_InstanceId(conv_id_df, Properties_activityId)
  dict_InitialStepAsync = get_instanceId_start_index(conv_id_df, Properties_InstanceId)
  dict_FinalStepAsync = get_instanceId_end_index(conv_id_df, Properties_InstanceId)
  dialog_index = map_instanceId_with_start_and_index(dict_InitialStepAsync, dict_FinalStepAsync)
  conv_id_df = map_dialog_and_AbandonedUserLeftDialog(conv_id_df, dialog_index, Properties_activityId, dict_InitialStepAsync)
  dialog_group_df = get_dialog_group(conv_id_df)
  dialog_group_df = map_dialogs_into_buckets(dialog_group_df)
  final_df = get_final_df(conv_id_df, dialog_group_df, all_dialogs_from_db)
  
  return final_df
  

# COMMAND ----------

def orchestration_function_process_data(mode,table, expected_source_data_columns_expr,  start_date=None, end_date=None):
    

    if mode.lower() == 'yesterday':
        yesterday = (datetime.now() + timedelta(hours=5, minutes=30)) - timedelta(days = 1)
        dates = [datetime.strftime(yesterday, '%Y-%m-%d')]
        
    else:
        dates = pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
        dates = [datetime.strftime(date_, '%Y-%m-%d') for date_ in dates]
    

    poc_df  = create_df(table,expected_source_data_columns_expr, dates) 
    filtered_conv = poc_df.where((F.col('conversationId').isNotNull()) & (F.col('conversationId').endswith("-in")))
    
    expected_schema = "Dialog: string, ConversationId: string, start_time: string, end_time: string, AbandonedUserLeftDialog: boolean, AbandonedUserCancelledDialog: boolean, FailedDialog: boolean, AgentDialog: boolean"
    
    final_output = filtered_conv.groupBy('date','conversationId').applyInPandas(apply_business_logic, expected_schema)
    
    return final_output
      

# COMMAND ----------

start_date = date(2022,11,10)
end_date = date(2022,11,11)

output = orchestration_function_process_data('',f"delta.`{config['database_path']}/batch/bronze`", expected_source_data_columns_expr, start_date, end_date)

# COMMAND ----------

output.write.format('delta').option('path',f"{config['main_directory']}/silver_jump").save()

# COMMAND ----------

display(output)
