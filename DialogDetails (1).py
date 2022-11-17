# Databricks notebook source
# MAGIC %run ./00setup

# COMMAND ----------

# mongodb_connection_string = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-mongo-conn-string')
# mongodb_database = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-mongo-dbName')
# mongodb_source_collection = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-mongo-collection')

# DialogDetailsTable = '[dbo].[AppInsightsDialogDetails]'

# jdbcHostname = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-sqlserver')
# jdbcPort = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-db-jdbc-port')
# jdbcDatabase = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-database')
# username = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-db-username')
# password = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-db-password')
# jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
# connectionProperties = {"user" : username,"password" : password,"driver" : jdbcDriver}


# COMMAND ----------

output_columns_names = ['Dialog', 'ConversationId', 'start_time', 'end_time', 'AbandonedUserLeftDialog', 'AbandonedUserCancelledDialog', 'FailedDialog', 'AgentDialog']

# COMMAND ----------

import pandas as pd
import numpy as np
import pymongo
from datetime import datetime, date, timedelta

# COMMAND ----------

# def fetch_data_from_mongoDB_by_convId(conversationId):
#     client = pymongo.MongoClient(mongodb_connection_string)
#     db = client[mongodb_database]
#     col = db[mongodb_source_collection]
    
#     myquery = {"$or": [{"Properties.conversationId" : conversationId}, 
#                               {"Properties.MasterBotConversationId" : conversationId}]}
#     mydoc = col.find(myquery)
#     data = list(mydoc)
#     return data

# COMMAND ----------

# def fetch_data_from_mongoDB_by_date(yesterday):
#     client = pymongo.MongoClient(mongodb_connection_string)
#     db = client[mongodb_database]
#     col = db[mongodb_source_collection]
#     myquery = {"time" : { "$regex": "^"+yesterday}}
#     mydoc = col.find(myquery)
#     data = list(mydoc)
#     return data

# COMMAND ----------

poc_df = spark.sql("select * from test_table1 where cast(time as Date) = '2022-11-10'").toPandas()

# COMMAND ----------

def create_df(data):
    # df = pd.DataFrame(data)
    df = data
    temp = df['properties'].apply(pd.Series)
    columns = ['Properties_'+x for x in list(temp.columns)]
    temp.columns = columns
    df = pd.concat([df.drop(['properties'], axis=1), temp], axis=1)
    del temp
    
    
    expected_source_data_columns = ['time', 'Name', 'Properties_conversationId', 
                                    'Properties_MasterBotConversationId',
                                    'Properties_text', 'Properties_activityId',
                                    'Properties_TopAnswer', 'Properties_TopAnswerScore',
                                    'Properties_intent', 'Properties_intentScore',
                                    'Properties_userUtterance',
                                    'Properties_DialogId', 'Properties_StepName',
                                    'Properties_InstanceId', 'AppRoleName']

    missing_columns = list(set(expected_source_data_columns) - set(df.columns))

    for i in missing_columns:
        df[i] = np.nan

    df = df[expected_source_data_columns]
        
    df['ConversationId'] = np.where(df['Properties_MasterBotConversationId'].isna(), 
                                    df.Properties_conversationId, 
                                    df.Properties_MasterBotConversationId)

#     df = df.sort_values(by=['Properties_activityId', 'time'])
    df = df.sort_values(by=['time', 'Properties_activityId'])

    df.reset_index(drop=True, inplace=True)

    return df


# COMMAND ----------

from collections import OrderedDict
def get_Properties_activityId(df):
    Properties_activityId = df[((df['Name'] == 'BotMessageReceived') & (df['Properties_text'].notnull()))]['Properties_activityId'].values
    return list(OrderedDict.fromkeys(Properties_activityId))

# COMMAND ----------

def get_Properties_InstanceId(df, Properties_activityId):
    Properties_InstanceId = df[(df['Properties_InstanceId'].notnull()) & (df.Properties_activityId.isin(Properties_activityId))]['Properties_InstanceId'].values
    return list(OrderedDict.fromkeys(Properties_InstanceId))


# COMMAND ----------

def get_instanceId_start_index(df, Properties_InstanceId):
    temp = df[(df['Properties_InstanceId'].isin(Properties_InstanceId)) 
              & ((df.Properties_StepName == 'InitialStepAsync') & (df.Properties_DialogId == 'MainDialog.WaterfallDialog')) 
              | ((df.Properties_StepName == 'EstablishChatStepAsync') & (df.Properties_DialogId == 'WaterfallDialog'))]

    index_InitialStepAsync = temp.index.tolist()
    Properties_InstanceId_InitialStepAsync = temp.Properties_InstanceId.tolist()

    return dict(zip(Properties_InstanceId_InitialStepAsync, index_InitialStepAsync))


# COMMAND ----------

def get_instanceId_end_index(df, Properties_InstanceId):
    temp = df[(df['Properties_InstanceId'].isin(Properties_InstanceId)) & 
              ((df.Properties_DialogId == 'DisambiguateUserUtteranceDialog.WaterfallDialog') 
               | (df.Properties_StepName == 'FinalStepAsync') 
               | (df.Name == 'WaterfallComplete'))]

    index_FinalStepAsync = temp.index.tolist()
    Properties_InstanceId_FinalStepAsync = temp.Properties_InstanceId.tolist()
    
    return dict(zip(Properties_InstanceId_FinalStepAsync, index_FinalStepAsync))

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

def map_dialogs_into_buckets(data):
    dialog_group_df = data.copy()
    dialog_group_df['AbandonedUserCancelledDialog'] = dialog_group_df.Properties_intent.apply(lambda x: 'Cancel' in x)
    dialog_group_df['FailedDialog'] = dialog_group_df.Name.apply(lambda x: 'FailedUtterance' in x)
    dialog_group_df['AgentDialog'] = dialog_group_df.Properties_StepName.apply(lambda x: 'EstablishChatStepAsync' in x)
    return dialog_group_df

# COMMAND ----------

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

def get_all_dialogs_from_db():
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    df = spark.read.jdbc(url=jdbcUrl, table='(select DISTINCT([Dialog]) from ' + DialogDetailsTable + ') query_wrap', properties=connectionProperties)
    return df.toPandas().Dialog.unique()



# COMMAND ----------

# Saving it in DB
def save_valid_conversations_to_db(output):
    try:
        # Need to change NaN to "NaN" and type of all columns as 'str' for spark dataframe
        output[output_columns_names] = np.where(output[output_columns_names].isna(), 'NaN', output[output_columns_names])
        tdf = output.astype(str)
        

        jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
        if not tdf.empty:
            spark_df = spark.createDataFrame(tdf)
            spark_df.write.jdbc(url=jdbcUrl,table=DialogDetailsTable,mode="append",properties=connectionProperties)
#             print('\t-- Inserted',len(tdf.ConversationId.unique()) , 'valid conversationids data. -- (Rows Inserted:', tdf.shape[0], ')')
            return True
        else:
            print("\tNo valid conversation data to save!!")
            return True
        
    except Exception as e:
        print('\tFailed while saving valid conversation dataframe to database')
        return False


# COMMAND ----------


# CREATE TABLE if not exists default.DialogDetails
# USING DELTA
# LOCATION 'abfss://deltalakepoc@statddevdecicaplrs05.dfs.core.windows.net/delta'




# COMMAND ----------

# spark.sql("""
# drop table dialogdetails
# """)

# COMMAND ----------



# COMMAND ----------

def orchestration_function_process_data(mode, start_date=None, end_date=None):
    all_dialogs_from_db = [] #get_all_dialogs_from_db()
#     output = pd.DataFrame(columns=output_columns_names)
    
    if mode.lower() == 'yesterday':
        yesterday = (datetime.now() + timedelta(hours=5, minutes=30)) - timedelta(days = 1)
        dates = [datetime.strftime(yesterday, '%Y-%m-%d')]
        
    else:
        dates = pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
        dates = [datetime.strftime(date_, '%Y-%m-%d') for date_ in dates]
        
    for date_ in dates:
        print('**** Processing data for date:', date_)
        # data = fetch_data_from_mongoDB_by_date(date_)
        # print("lol",poc_df)
        if len(poc_df) > 0:
            df = create_df(poc_df)
            nth_day_conv_ids = df.ConversationId.unique()
            nth_day_conv_ids = [conv_id for conv_id in nth_day_conv_ids if not pd.isnull(conv_id) and conv_id.endswith("-in")]
            print('\tTotal conversations:', len(nth_day_conv_ids))
            
            bounced_counter = 0
            valid_counter = 0
            for conv_id in nth_day_conv_ids:
                try:
                    conv_id_df = df[df['ConversationId'] == conv_id]
                    Properties_activityId = get_Properties_activityId(conv_id_df)
                    if len(Properties_activityId) == 0:
                        bounced_counter = bounced_counter + 1
                        continue
                    if not conv_id.endswith("-in"):
                        continue
                    Properties_InstanceId = get_Properties_InstanceId(conv_id_df, Properties_activityId)
                    dict_InitialStepAsync = get_instanceId_start_index(conv_id_df, Properties_InstanceId)
                    dict_FinalStepAsync = get_instanceId_end_index(conv_id_df, Properties_InstanceId)
                    dialog_index = map_instanceId_with_start_and_index(dict_InitialStepAsync, dict_FinalStepAsync)
                    conv_id_df = map_dialog_and_AbandonedUserLeftDialog(conv_id_df, dialog_index, Properties_activityId, dict_InitialStepAsync)
                    dialog_group_df = get_dialog_group(conv_id_df)
                    dialog_group_df = map_dialogs_into_buckets(dialog_group_df)
                    final_df = get_final_df(conv_id_df, dialog_group_df, all_dialogs_from_db)
                    if len(final_df) > 0:
                        # save_valid_conversations_to_db(final_df)
                        # write pandas df to hive table
                        
                        write_final_df = spark.createDataFrame(final_df)
                        # write_final_df.display()
                        (write_final_df
                        .write.format("delta")
                        .mode("append")
                        .option("mergeSchema","true")
                        .option('path',bronze_land+"dialog_newfolder").save())
                        # .saveAsTable("default.dialogdetails"))
                        # print("lol")

                        valid_counter = valid_counter + 1
#                         output = output.append(final_df, ignore_index=True)

                except Exception as e:
                    print('\tFailed for Conversation', conv_id)
                    print(e)

            print('\t-- Bounced conversations count:', bounced_counter)
            print('\t-- Saved conversations count:', valid_counter)
                    
        else:
            print('\tNo data found for this date')
    
    return True
#     return output


# COMMAND ----------

# output = orchestration_function_process_data('yesterday')


# COMMAND ----------

start_date = date(2022,11,10)
end_date = date(2022,11,11)

output = orchestration_function_process_data('', start_date, end_date)

# COMMAND ----------

spark.sql(f""" 
CREATE TABLE if not exists default.DialogDetailsNewFolder
USING DELTA
LOCATION '{bronze_land}/dialog_newfolder'
""")

# COMMAND ----------

# dbutils.fs.rm(f'{bronze_land}/dialog_test',True)
