# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

# MAGIC %run ./temp

# COMMAND ----------

# display(poc_df_temp.where(F.col('properties.activityId')=='6EjzeJXcqOqE84mm1jtomd-in|0000012'))

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pyspark.sql.functions as F
from collections import OrderedDict


# COMMAND ----------

# poc_df_temp.where((F.col('conversationId').isNotNull()) & (F.col('conversationId').endswith("-in") & (F.col('MasterBotConversationId')==""))).display()

# COMMAND ----------

output_columns_names = ['timestamp', 'Properties_text', 'activityId', 'Intent', 'Intent_score', 'AppRoleName', 'Child_AppRoleName', 'Properties_conversationId', 'Properties_MasterBotConversationId', 'ConversationId']

exclude_intent = ['ReadAloud','Reject','Repeat','SearchByDate','SearchByOrderId','SelectAny','SelectItem','SelectNone','ShowPrevious','StartOver','Stop','Cancel','Confirm','DoNotRemember','ExtractEmail','ExtractNumber','FinishTask','GoBack','Help','ExtractName']

agent_connection_start_string = "You're now connected to a customer care executive."
agent_connection_end_string = "Your conversation with the customer care executive has ended."

user_utterance_to_remove = ['cancel', 'show more', 'yes, please.']

agent_connection_strings = ['i would like to talk to a customer care executive instead.',
                           'no, i want to talk to a customer care executive.',
                           'customer care',
                           'i want to talk to a customer care executive.',
                           'i want to talk to an agent',
                           'i want to chat with support team',
                           'need to talk to executive',
                           'i want to connect to an agent',
                           'connect to an customer care executive!',
                           'i want to talk to customer care']

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
    "AppRoleName" : "AppRoleName"
}

output_columns = ['timestamp','Properties_text','activityId','Intent','Intent_score','AppRoleName','Child_AppRoleName','Properties_conversationId','Properties_MasterBotConversationId','ConversationId','Agent_Connection','master_score','child_score','final_score']

expected_output_schema= """timestamp: string, Properties_text: string, activityId: string, Intent: string, Intent_score: string, AppRoleName: string, Child_AppRoleName: string, 
    Properties_conversationId: string, Properties_MasterBotConversationId: string, ConversationId: string, Agent_Connection: string, master_score: string, child_score: string, final_score: string"""

# COMMAND ----------

def create_df(data_table, expected_source_data_columns_expr, date_range):
  """
  
  """
    # missing column logic to be added if required
  base_df = (
    # spark.table(data_table)
            poc_df_temp
             .withColumn('date', F.to_date(F.col('time')))
          .where(F.col('date').isin(date_range)) #  filter for required date range if required
         )
  
  
  for _as,to in expected_source_data_columns_expr.items():
    base_df = base_df.withColumn(to,F.col(_as))

  final_df = base_df.withColumn("ConversationId",F.coalesce(F.col("Properties_MasterBotConversationId"),F.col("Properties_conversationId")))
  return final_df

# COMMAND ----------

def get_Properties_activityId(df):

   
    Properties_activityId = df[((df['Name'] == 'BotMessageReceived') & (df['Properties_text'].notnull()))
                               |(df.Properties_text == agent_connection_start_string)
                               |(df.Properties_text == agent_connection_end_string)]['Properties_activityId'].values

    return list(OrderedDict.fromkeys(Properties_activityId))

def get_intent(df_activity_id):
    
    name = None
    Properties_intent = None
    Properties_intentScore = None
    AppRoleName = None
    luis_score = 0
    qna_score = 0
    Properties_conversationId = ''
    Properties_MasterBotConversationId = ''
    agent_connection = False
    agent_closed = False
    
    if agent_connection_start_string in list(df_activity_id.Properties_text):
        agent_connection = True
        
    if agent_connection_end_string in list(df_activity_id.Properties_text):
        agent_closed = True
    
    if 'FailedUtterance' in list(df_activity_id.Name):
        Properties_conversationId = df_activity_id.loc[df_activity_id.Name == 'FailedUtterance'].Properties_conversationId.iloc[0]
        Properties_MasterBotConversationId = df_activity_id.loc[df_activity_id.Name == 'FailedUtterance'].Properties_MasterBotConversationId.iloc[0]
        AppRoleName = df_activity_id.loc[df_activity_id.Name == 'FailedUtterance'].AppRoleName.iloc[0]

        return {
            'name': 'FailedUtterance',
            'Properties_intent': 'FailedUtterance',
            'Properties_intentScore': 0,
            'AppRoleName': AppRoleName,
            'Properties_conversationId': Properties_conversationId,
            'Properties_MasterBotConversationId': Properties_MasterBotConversationId,
            'Agent_connection': agent_connection,
            'Agent_closed': agent_closed
        }
        
    temp_luis = df_activity_id.loc[df_activity_id.Name == 'LuisResult']

    if temp_luis.shape[0] > 0:
        shortlisted = temp_luis[~temp_luis.Properties_intent.isin(exclude_intent)]

        if shortlisted.shape[0]>0:
            name = shortlisted.iloc[-1].Name
            Properties_intent = shortlisted.iloc[-1].Properties_intent
            luis_score = shortlisted.iloc[-1].Properties_intentScore
            AppRoleName = shortlisted.iloc[-1].AppRoleName
            Properties_conversationId = shortlisted.iloc[-1].Properties_conversationId
            Properties_MasterBotConversationId = shortlisted.iloc[-1].Properties_MasterBotConversationId
        else:
            AppRoleName = temp_luis.dropna(subset=['Properties_intent']).AppRoleName.iloc[0]
            Properties_conversationId = temp_luis.dropna(subset=['Properties_intent']).Properties_conversationId.iloc[0]
            Properties_MasterBotConversationId = temp_luis.dropna(subset=['Properties_intent']).Properties_MasterBotConversationId.iloc[0]

    else:
        AppRoleName = df_activity_id.AppRoleName.iloc[0]
        Properties_conversationId = df_activity_id.Properties_conversationId.iloc[0]
        Properties_MasterBotConversationId = df_activity_id.Properties_MasterBotConversationId.iloc[0]          

            
    temp_qna = df_activity_id.loc[df_activity_id.Name == 'QnAResult']

    if temp_qna.shape[0] > 0:
        qna_score = temp_qna.iloc[0].Properties_TopAnswerScore
        AppRoleName = temp_qna.iloc[0].AppRoleName

    if float(qna_score) > float(luis_score):
        name = 'QnAResult'
        Properties_intent = 'QnAResult'
        Properties_intentScore = qna_score
        Properties_conversationId = temp_qna.iloc[0].Properties_conversationId
        Properties_MasterBotConversationId = temp_qna.iloc[0].Properties_MasterBotConversationId
    
    else:
        Properties_intentScore = luis_score
        
        
    return {
        'name': name,
        'Properties_intent': Properties_intent,
        'Properties_intentScore': Properties_intentScore,
        'AppRoleName': AppRoleName,
        'Properties_conversationId': Properties_conversationId,
        'Properties_MasterBotConversationId': Properties_MasterBotConversationId,
        'Agent_connection' : agent_connection,
        'Agent_closed': agent_closed
    }


def process_each_activity_id_and_generate_df(Properties_activityId, df):
    time = []
    text = []
    activityId = []
    intent = []
    score = []
    app_name = []
    child_app_name = []
    conv_id = []
    master_bot_conv_id = []
    agent_connection = []
    agent_closed = []
    
    for activityid in Properties_activityId:
        df_activity_id = df[df['Properties_activityId'] == activityid][df['Name'].isin(['BotMessageReceived', 'LuisResult', 'QnAResult', 'UtteranceDisambiguated', 'FailedUtterance', 'BotMessageSend'])]
        
        try:
          current_intent = get_intent(df_activity_id)

          time.append(df_activity_id[df['Name'] == 'BotMessageReceived']['time'].values[0])
          text.append(df_activity_id[df['Name'] == 'BotMessageReceived']['Properties_text'].values[0])
          activityId.append(activityid)
          intent.append(current_intent['Properties_intent'])
          score.append(current_intent['Properties_intentScore'])
          
          app_name.append(df_activity_id[df['Name'] == 'BotMessageReceived']['AppRoleName'].values[0])
          
          child_app_name.append(current_intent['AppRoleName'])
          conv_id.append(current_intent['Properties_conversationId'])
          master_bot_conv_id.append(current_intent['Properties_MasterBotConversationId'])
          agent_connection.append(current_intent['Agent_connection'])
          agent_closed.append(current_intent['Agent_closed'])
        except Exception as e: 
          continue

    #     if activityid == 'BJcPPvMctYeHaRF31fIO0D-in|0000005':
    #       final_df = (pd.DataFrame(zip(time, text, activityId, intent, score, app_name, child_app_name, conv_id, master_bot_conv_id, agent_connection, agent_closed),
    # columns=['timestamp', 'Properties_text', 'activityId', 'Intent', 'Intent_score', 'AppRoleName', 'Child_AppRoleName', 
    # 'Properties_conversationId', 'Properties_MasterBotConversationId', 'Agent_Connection', 'Agent_Closed']))

    #       print(
    #         f"activityid in function ->  {activityid} and values = {list(zip(time, text, activityId, intent, score, app_name, child_app_name, conv_id, master_bot_conv_id, agent_connection, agent_closed))}",file=sys.stderr)

    #       print(f"and the created dataframe is {final_df.head()}")
        
    final_df = (pd.DataFrame(zip(time, text, activityId, intent, score, app_name, child_app_name, conv_id, master_bot_conv_id, agent_connection, agent_closed),
    columns=['timestamp', 'Properties_text', 'activityId', 'Intent', 'Intent_score', 'AppRoleName', 'Child_AppRoleName', 
    'Properties_conversationId', 'Properties_MasterBotConversationId', 'Agent_Connection', 'Agent_Closed']))

    
    final_df['ConversationId'] = np.where(final_df['Properties_MasterBotConversationId'].isna(), final_df.Properties_conversationId, final_df.Properties_MasterBotConversationId)

    return final_df


# COMMAND ----------

def compute_score(row):
    AppRoleName = row.AppRoleName
    Child_AppRoleName = row.Child_AppRoleName
    child_Intent = row.Intent
    Child_IntentScore = float(row.Intent_score) if row.Intent_score != None else row.Intent_score
    Agent_Connection = row.Agent_Connection
    
    if child_Intent == None:
        master_score = 0.0
    elif AppRoleName != Child_AppRoleName and child_Intent != 'FailedUtterance':
        master_score = 0.5 
#     in master_bot with good score
    elif child_Intent == 'QnAResult' and Child_AppRoleName == 'TDL.Chatbot.Master' and Child_IntentScore >=0.75:
        master_score = 0.5
#     in master_bot with bad score
    elif child_Intent == 'QnAResult' and Child_AppRoleName == 'TDL.Chatbot.Master' and 0.01 <= Child_IntentScore <= 0.75:
        master_score = 0.3
#     in childbot
    elif child_Intent == 'QnAResult' and Child_AppRoleName != 'TDL.Chatbot.Master' and 'TCP.Chatbot.' in Child_AppRoleName:
        master_score = 0.5
    elif child_Intent == 'IdentifyBrand' or (AppRoleName == Child_AppRoleName and child_Intent != 'FailedUtterance'):
        master_score = 0.3
    else:
        master_score = 0.0
        
    if Agent_Connection and AppRoleName == Child_AppRoleName:
        master_score = master_score - 0.1

    if child_Intent == None:
        child_score = 0.0
#     in master_bot
    elif child_Intent == 'QnAResult' and Child_AppRoleName == 'TDL.Chatbot.Master':
        child_score = 0.0
#     in childbot with good score
    elif child_Intent == 'QnAResult' and Child_AppRoleName != 'TDL.Chatbot.Master' and 'TCP.Chatbot.' in Child_AppRoleName and Child_IntentScore >=0.75:
        child_score = 0.5
#     in childbot with bad score
    elif child_Intent == 'QnAResult' and Child_AppRoleName != 'TDL.Chatbot.Master' and 'TCP.Chatbot.' in Child_AppRoleName and 0.01 <= Child_IntentScore <= 0.75:
        child_score = 0.35
    elif AppRoleName != Child_AppRoleName and Child_IntentScore >= 0.75:
        child_score = 0.5
    elif AppRoleName != Child_AppRoleName and 0.01 <= Child_IntentScore <= 0.75:
        child_score = 0.35
    else:
        child_score = 0.0
        
        
    if Agent_Connection and AppRoleName != Child_AppRoleName:
        child_score = child_score - 0.1
    
    master_score = round(master_score, 2)
    child_score = round(child_score, 2)
        
    return pd.Series([master_score, child_score])


# COMMAND ----------



# COMMAND ----------

def get_master_and_child_scores(final_df):
    final_df[['master_score', 'child_score']] = final_df.apply(compute_score, axis=1)
    text_to_remove = ['yes', 'no', 'no, thanks.']
    final_df.Properties_text.fillna(value=np.nan, inplace=True) # added to replicate ealier behaviour, where nan will be filtered in the next comparison. right now after conversion we get None object which does nor filter out for some reason

    final_df = final_df[~final_df['Properties_text'].str.lower().isin([x.lower() for x in text_to_remove])]

    final_df.reset_index(drop=True, inplace=True)
    
    return final_df
  
def process_IdentifyBrand_intent(final_df):
    valid_brands = ['croma', 'bigbasket', 'tata 1mg', 'tata neu', 'tata cliq', 'airasia india', 'ihcl', 'westside', 'qmin']
    rows_to_delete = []

    for i in range(final_df.shape[0] - 1):
        ConversationId = final_df['ConversationId'][i]
        Intent = final_df['Intent'][i]
        AppRoleName = final_df['AppRoleName'][i]
        Child_AppRoleName = final_df['Child_AppRoleName'][i]

        next_ConversationId = final_df['ConversationId'][i+1]
        next_Intent = final_df['Intent'][i+1]
        next_Properties_text = final_df['Properties_text'][i+1]

        if ConversationId == next_ConversationId and \
            Intent == 'IdentifyBrand' and \
            AppRoleName == Child_AppRoleName and \
            next_Intent != 'FailedUtterance' and \
            not pd.isnull(next_Properties_text) and \
            next_Properties_text.lower().strip() in valid_brands:

            final_df['Intent'][i] = final_df['Intent'][i+1]
            final_df['Intent_score'][i] = final_df['Intent_score'][i+1]
            final_df['Child_AppRoleName'][i] = final_df['Child_AppRoleName'][i+1]
            final_df['child_score'][i] = final_df['child_score'][i+1]
            final_df['Properties_conversationId'][i] = final_df['Properties_conversationId'][i+1]
            final_df['Properties_MasterBotConversationId'][i] = final_df['Properties_MasterBotConversationId'][i+1]
            rows_to_delete.append(i+1)

    final_df = final_df.drop(rows_to_delete,axis='index')
    final_df.reset_index(drop=True, inplace=True)
    
    return final_df


# COMMAND ----------

def process_agent_connection(final_df):

    rows_to_delete = []

    for i in range(final_df.shape[0] - 1):
        ConversationId = final_df['ConversationId'][i]
        AppRoleName = final_df['AppRoleName'][i]
        Child_AppRoleName = final_df['Child_AppRoleName'][i]
        
        next_ConversationId = final_df['ConversationId'][i+1]
        next_Properties_text = final_df['Properties_text'][i+1]
        next_Agent_Connection = final_df['Agent_Connection'][i+1]
        
        if ConversationId == next_ConversationId and \
            (pd.isnull(next_Properties_text) or next_Properties_text.lower() == 'yes, please.') and \
            next_Agent_Connection == True:
            final_df['Agent_Connection'][i] = final_df['Agent_Connection'][i+1]
            if AppRoleName == Child_AppRoleName:
                final_df['master_score'][i] = round(final_df['master_score'][i] - 0.1, 2)
            if AppRoleName != Child_AppRoleName:
                final_df['child_score'][i] = round(final_df['child_score'][i] - 0.1, 2)
            rows_to_delete.append(i+1)
    
    final_df = final_df.drop(rows_to_delete,axis='index')
    final_df.reset_index(drop=True, inplace=True)

    return final_df
  
def map_agent_connection(final_df):
    rows_to_delete = []
    final_df['Agent_Connection_String'] = final_df['Properties_text'].str.lower().isin([x.lower() for x in agent_connection_strings])
    
    for i in range(final_df.shape[0] - 1):
        ConversationId = final_df['ConversationId'][i]
        AppRoleName = final_df['AppRoleName'][i]
        Child_AppRoleName = final_df['Child_AppRoleName'][i]

        next_ConversationId = final_df['ConversationId'][i+1]
        next_Agent_Connection = final_df['Agent_Connection'][i+1]
        next_Agent_Connection_String = final_df['Agent_Connection_String'][i+1]

        if ConversationId == next_ConversationId and next_Agent_Connection and next_Agent_Connection_String:
            final_df['Agent_Connection'][i] = final_df['Agent_Connection'][i+1]
            if AppRoleName == Child_AppRoleName:
                final_df['master_score'][i] = round(final_df['master_score'][i] - 0.1, 2)
            if AppRoleName != Child_AppRoleName:
                final_df['child_score'][i] = round(final_df['child_score'][i] - 0.1, 2)
            rows_to_delete.append(i+1)

    final_df = final_df.drop(rows_to_delete,axis='index')
    final_df.reset_index(drop=True, inplace=True)

    return final_df

# COMMAND ----------

def delete_conversation_between_agent_and_user(final_df):
    final_df.reset_index(drop=True, inplace=True)
    rows_to_delete = []
    
    for conv_id in final_df['ConversationId'].unique():
        conv_id_df = final_df[final_df['ConversationId'] == conv_id]

        connection_index = conv_id_df[conv_id_df['Agent_Connection']].index.tolist()
        closed_index = conv_id_df[conv_id_df['Agent_Closed']].index.tolist()

        if len(connection_index) > 0 and len(connection_index) == len(closed_index):
            for i in range(len(connection_index)):
                connection_start_index = connection_index[i]
                connection_closed_index = closed_index[i]
                rows_to_delete.extend([i+1 for i in range(connection_start_index, connection_closed_index)])


    final_df = final_df.drop(rows_to_delete,axis='index')
    final_df.reset_index(drop=True, inplace=True)
    return final_df
        
def prepare_final_df(final_df, all_activity_ids_from_db):
    final_df['final_score'] = round((final_df['master_score'] + final_df['child_score']), 2)
    final_df['ConversationId'] = np.where(final_df['Properties_MasterBotConversationId'].isna(), final_df.Properties_conversationId, final_df.Properties_MasterBotConversationId)

    final_df = final_df[~final_df['Properties_text'].str.lower().isin([x.lower() for x in user_utterance_to_remove])]
    
    final_df = final_df[['timestamp', 'Properties_text', 'activityId', 
                         'Intent', 'Intent_score', 'AppRoleName', 'Child_AppRoleName', 
                         'Properties_conversationId','Properties_MasterBotConversationId', 'ConversationId',
                         'Agent_Connection',
                         'master_score', 'child_score','final_score']]
    
    # final_df = final_df[~final_df.activityId.isin(all_activity_ids_from_db)]
    # final_df.reset_index(drop=True, inplace=True)

    return final_df


# COMMAND ----------


def apply_transformation_logic(conv_id_df):
  """
  """
  try:
    warnings.filterwarnings('ignore')
    conv_id_df = conv_id_df.sort_values(by=['time', 'Properties_activityId'])
    conv_id_df.reset_index(drop=True, inplace=True)
    Properties_activityId = get_Properties_activityId(conv_id_df)

    if len(Properties_activityId) == 0:
      return pd.DataFrame(columns=output_columns)

    final_df = process_each_activity_id_and_generate_df(Properties_activityId, conv_id_df)
    final_df = get_master_and_child_scores(final_df)
    final_df = process_IdentifyBrand_intent(final_df)
    final_df = process_agent_connection(final_df)
    final_df = map_agent_connection(final_df)
    final_df = delete_conversation_between_agent_and_user(final_df)
    final_df = prepare_final_df(final_df, [])
    final_df = final_df.astype(str)
  except Exception as e:
    print(f"exception captured {e}",file=sys.stderr)
    return pd.DataFrame(columns=output_columns)

  return final_df


# COMMAND ----------

def orchestration_function_process_data(mode,table, expected_source_data_columns_expr,expected_output_schema,  start_date=None, end_date=None):

    if mode.lower() == 'yesterday':
        yesterday = (datetime.now() + timedelta(hours=5, minutes=30)) - timedelta(days = 1)
        dates = [datetime.strftime(yesterday, '%Y-%m-%d')]
    else:
        dates = pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
        dates = [datetime.strftime(date_, '%Y-%m-%d') for date_ in dates]

    poc_df  = create_df(table,expected_source_data_columns_expr, dates)

    filtered_conv = poc_df.where((F.col('conversationId').isNotNull()) & (F.col('conversationId').endswith("-in")))
    print(f"+++++++++++ NEW RUN +++++++++++",file=sys.stderr)
    final_output = (filtered_conv
                    .groupBy('date','conversationId')
                    .applyInPandas(apply_transformation_logic, expected_output_schema))
    
    return final_output
      

# COMMAND ----------

start_date = date(2022, 11, 10)
end_date = date(2022, 11, 11)

output = orchestration_function_process_data('',f"delta.`{config['database_path']}/batch/bronze`", expected_source_data_columns_expr,expected_output_schema, start_date, end_date)

display(output)#.where(F.col('activityId')== 'BJcPPvMctYeHaRF31fIO0D-in|0000005'))
# output.write.mode('overwrite').option('overwriteSchema','true').format('delta').option('path',f"{config['main_directory']}/silver_jump_usecase2").save()


# COMMAND ----------

output.select('activityId').distinct().count()

# COMMAND ----------

output.where(F.col('activityId').isin(['8GeIeR5xUiZ8y3MYkZDePH-in|0000001','8GeIeR5xUiZ8y3MYkZDePH-in|0000011','6EjzeJXcqOqE84mm1jtomd-in|0000012'])).display()

# COMMAND ----------

# 1. activity id duplicates

# COMMAND ----------

# def  write_first_time(df, target_location):
#   """
#   partition if necessary
#   """
#   (df.write.format('delta').mode('overwrite')
#    .option('overwriteSchema','true').option('path', target_location)
#    .save())
#   return None

# def write_data_frame(updates, target_table):
#   """ 
#   partition if necessary
#   """
#   try:  
#     output_table = DeltaTable.forPath(spark, target_table)                                                                                        
#     (output_table
#      .alias("t")
#      .merge(
#         updates.alias("s"), 
#         "t.ConversationId = s.ConversationId  and t.activityId = s.activityId")
#      .whenMatchedUpdateAll()
#      .whenNotMatchedInsertAll()
#      .execute())
#   except AnalysisException as e:
#     if e.getErrorClass() == 'DELTA_MISSING_DELTA_TABLE':
#       write_first_time(updates, target_table)
#     else:
#        print(f"Merge failed {e}", file=sys.stderr)
        
#     return None  

# COMMAND ----------


