# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

silverTable = spark.table(f"delta.`{config['database_path']}"+"/silver_table`")

# COMMAND ----------

# add CDF to final table
# add expectations library for demo 

# drive towards usage of 

# COMMAND ----------

['AppVersion','AppRoleName', 'clientIP', 'ClientType', 'ClientCity', 'Name','OperationId','ParentId','UserId','SessionId', '_BilledSize']


# COMMAND ----------

display(silverTable)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   latest_date,
# MAGIC   COUNT(DISTINCT Properties_CustomerHash) AS 'Total unique users',
# MAGIC   COUNT(
# MAGIC     DISTINCT CASE
# MAGIC       WHEN days_diff = 1 THEN Properties_CustomerHash
# MAGIC     END
# MAGIC   ) AS 'Total repeated unique users wrt prev day' INTO ##all_customer_activity
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       latest_date,
# MAGIC       Properties_CustomerHash,
# MAGIC       lag(latest_date) over(
# MAGIC         partition by Properties_CustomerHash
# MAGIC         ORDER BY
# MAGIC           latest_date
# MAGIC       ) AS prev_date,
# MAGIC       DATEDIFF(
# MAGIC         DAY,
# MAGIC         lag(latest_date) over(
# MAGIC           partition by Properties_CustomerHash
# MAGIC           ORDER BY
# MAGIC             latest_date
# MAGIC         ),
# MAGIC         latest_date
# MAGIC       ) AS days_diff
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           DISTINCT CAST(time AS Date) AS latest_date,
# MAGIC           Properties_CustomerHash
# MAGIC         FROM
# MAGIC           AppInsightsUserAdded
# MAGIC         WHERE
# MAGIC           CAST(time AS Date) >= CAST(
# MAGIC             DATEADD(
# MAGIC               DAY,
# MAGIC               -3,
# MAGIC               CAST(SWITCHOFFSET(GETUTCDATE(), '+05:30') AS DATE)
# MAGIC             ) AS Date
# MAGIC           )
# MAGIC       ) t
# MAGIC   ) t
# MAGIC GROUP BY
# MAGIC   latest_date
# MAGIC END BEGIN
# MAGIC SELECT
# MAGIC   CAST(time AS Date) AS latest_date,
# MAGIC   COUNT(*) AS 'Total number of users' INTO ##total_users
# MAGIC FROM
# MAGIC   AppInsightsUserAdded
# MAGIC WHERE
# MAGIC   CAST(time AS Date) = CAST(
# MAGIC     DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(SWITCHOFFSET(GETUTCDATE(), '+05:30') AS DATE)
# MAGIC     ) AS Date
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   CAST(time AS Date)
# MAGIC SELECT
# MAGIC   CAST(time AS date) AS date,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Name = 'FailedUtterance' THEN COALESCE(
# MAGIC         Properties_MasterBotConversationId,
# MAGIC         Properties_conversationId
# MAGIC       )
# MAGIC     END
# MAGIC   ) AS 'Total number of failed conversations',
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Name = 'UtteranceDisambiguated' THEN COALESCE(
# MAGIC         Properties_MasterBotConversationId,
# MAGIC         Properties_conversationId
# MAGIC       )
# MAGIC     END
# MAGIC   ) AS 'Total number of disambiguated conversations' INTO ##failed_conversations
# MAGIC FROM
# MAGIC   AppInsightsFailedUtterances
# MAGIC WHERE
# MAGIC   CAST(time AS Date) = CAST(
# MAGIC     DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(SWITCHOFFSET(GETUTCDATE(), '+05:30') AS DATE)
# MAGIC     ) AS Date
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   CAST(time AS date)
# MAGIC END BEGIN
# MAGIC SELECT
# MAGIC   CAST(starttime AS Date) AS Date,
# MAGIC   SUM(time_diff) AS 'Conversation Duration(mins)' INTO ##converstaion_duration
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       Conversationid,
# MAGIC       Properties_activityId,
# MAGIC       starttime,
# MAGIC       endtime,
# MAGIC       DATEDIFF(SECOND, starttime, endtime) * 1.00 / 60 AS time_diff
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           DISTINCT COALESCE(
# MAGIC             Properties_MasterBotConversationId,
# MAGIC             Properties_conversationId
# MAGIC           ) AS Conversationid,
# MAGIC           Properties_activityId,
# MAGIC           MIN(timestamp) AS starttime,
# MAGIC           MAX(timestamp) AS ENDtime
# MAGIC         FROM
# MAGIC           d
