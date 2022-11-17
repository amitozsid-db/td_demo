# Databricks notebook source
# MAGIC %run ./00setup

# COMMAND ----------

## Import Libraries
import pandas as pd 
from datetime import date,datetime
from datetime import timedelta
import pytz

# COMMAND ----------

RowsInserted = 0

# COMMAND ----------

# # SQL Data Source
# jdbcHostname = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-sqlserver')
# jdbcPort = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-db-jdbc-port')
# jdbcDatabase = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-database')
# username = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-db-username')
# password = dbutils.secrets.get(scope = 'USMSecretScope' ,key = 'cha-db-password')
# jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
# connectionProperties = {"user" : username,"password" : password,"driver" : jdbcDriver}

# tableName = "AppInsightsDialogDetails"
# updateTable = "AppInsightsDialogSummary"

# COMMAND ----------

def get_dialog_details_from_db(date):
    # jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    # sql_query = "select * from " + str(tableName) + " where cast(start_time as Date) = '" + str(date) + "'"
    # df = spark.read.jdbc(url=jdbcUrl, table="("+ sql_query +") query_wrap", properties=connectionProperties)
    
    #get data from saved table in Dialog Details
    df = spark.sql(
      """
      Select * from default.dialogdetailsnewfolder
      """
    )

    return df.toPandas()

# COMMAND ----------

# Saving it in DB
# @measure_time
def save_dialog_summary(output):
    try:
        global RowsInserted
        tdf = output.astype(str)
        # jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
        if not tdf.empty:
            # spark_df = spark.createDataFrame(tdf)
            # spark_df.write.jdbc(url=jdbcUrl,table=updateTable,mode="append",properties=connectionProperties)

            write_final_df = spark.createDataFrame(tdf)
            (write_final_df
                        .write.format("delta")
                        .mode("append")
                        .option("mergeSchema","true")
                        .option('path',bronze_land+"dialog_summary").save())

            RowsInserted = RowsInserted + 1
#             print('\t-- Inserted row')
            return True
        else:
            print("\tNo Valid data to save!!")
            return True
        
    except Exception as e:
        print('\tFailed while saving valid conversation dataframe to database')
        print(e)
        return False

# COMMAND ----------

spark.sql(f""" 
CREATE TABLE if not exists default.DialogSummary
USING DELTA
LOCATION '{bronze_land}/dialog_summary'
""")

# COMMAND ----------

# @measure_time
def get_conversation_details(mode, start_date=None, end_date=None):
    
    final_output = pd.DataFrame()
    
    IST = pytz.timezone('Asia/Kolkata')
    
    if mode.lower() == 'yesterday':
        yesterday = datetime.now(IST).date() - timedelta(days = 1)
        dates = [datetime.strftime(yesterday, '%Y-%m-%d')]
    else:
        dates = pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
        dates = [datetime.strftime(date_, '%Y-%m-%d') for date_ in dates]
        
    for date_ in dates:
        summary_df = pd.DataFrame()
        filtered_df = get_dialog_details_from_db(date_)
        abandoned_dialogs = filtered_df[((filtered_df['AbandonedUserLeftDialog'] == 'True') | (filtered_df['AbandonedUserCancelledDialog'] == 'True')) & (filtered_df['AgentDialog'] == 'False')]
        failed_dialogs = filtered_df[filtered_df['FailedDialog'] == 'True']
        agent_escalations =  filtered_df[(filtered_df['AgentDialog'] == 'True') & (filtered_df['AbandonedUserCancelledDialog'] == 'False')]
        
        open_agent_connection = filtered_df[(filtered_df['AgentDialog'] == 'True') & (filtered_df['AbandonedUserCancelledDialog'] == 'False') & (filtered_df['AbandonedUserLeftDialog'] == 'True')]
        closed_agent_connection = filtered_df[(filtered_df['AgentDialog'] == 'True') & (filtered_df['AbandonedUserCancelledDialog'] == 'False') & (filtered_df['AbandonedUserLeftDialog'] == 'False')]
    
        summary_df.loc[0,"Date"] = date_
        summary_df.loc[0,"TotalConversations"] = len(filtered_df['ConversationId'].unique())
        summary_df.loc[0,"TotalDialogs"] = len(filtered_df)
        summary_df.loc[0,"AbandonedDialogs"] = len(abandoned_dialogs)
        summary_df.loc[0,"FailedDailogs"] = len(failed_dialogs)
        summary_df.loc[0,"AgentDialogs"] = len(agent_escalations)
        summary_df.loc[0,"OpenAgentDialogs"] = len(open_agent_connection)
        summary_df.loc[0,"ClosedAgentDialogs"] = len(closed_agent_connection)
        summary_df.loc[0,"FCRDialogs"] = len(filtered_df) - (len(abandoned_dialogs) + len(failed_dialogs) + len(agent_escalations))
        summary_df.loc[0,"FCRPct"] = round((summary_df.loc[0,"FCRDialogs"] / (summary_df.loc[0,"TotalDialogs"] - summary_df.loc[0,"AbandonedDialogs"]))*100,2) if summary_df.loc[0,"TotalDialogs"] > 0 else 0 
        summary_df.loc[0,"FailedPct"] = round((summary_df.loc[0,"FailedDailogs"] / (summary_df.loc[0,"TotalDialogs"] - summary_df.loc[0,"AbandonedDialogs"]))*100,2) if summary_df.loc[0,"TotalDialogs"] > 0 else 0 
        summary_df.loc[0,"FBRPct"] = round((summary_df.loc[0,"AgentDialogs"] / (summary_df.loc[0,"TotalDialogs"] - summary_df.loc[0,"AbandonedDialogs"]))*100,2) if summary_df.loc[0,"TotalDialogs"] > 0 else 0 
        
        final_output = final_output.append(summary_df, ignore_index=True)
        
        if summary_df.shape[0] > 0:
            save_dialog_summary(summary_df)
        
    return final_output

# COMMAND ----------

start_date = date(2022,11,10)
end_date = date(2022,11,11)

output = get_conversation_details('', start_date, end_date)


# output = get_conversation_details('yesterday')


# COMMAND ----------

dbutils.notebook.exit(RowsInserted)
