# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

# MAGIC %pip install great_expectations

# COMMAND ----------

import pyspark.sql.functions as F
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

spark.sql(f"USE DATABASE {config['database']}")

# COMMAND ----------

#we can add NOT NULL in our ID field (or even more advanced constraint)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {config['database']}.client_type_distribution (
  date DATE NOT NULL,
  clientType STRING,
  userBase LONG,
  totalUsers LONG,
  percentUsersCap STRING
) TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {config['database']}.user_prefered_channel (
  date DATE NOT NULL,
  userId STRING,
  activityType STRING,
  interactions DOUBLE,
  communicationChannel ARRAY<STRING>
) TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)""")

# COMMAND ----------

# Dropping following columns ['AppVersion','_metadata','row_created_metadata','ClientStateOrProvince','ClientCountryOrRegion','OperationId','SDKVersion','ParentId']

silver_table = spark.table(f"delta.`{config['database_path']}"+"/silver_table`")

select_cols = ['time','AppRoleInstance','AppRoleName','AppVersion','ClientType','Type','clientBrowser', 'clientIP',  'ClientCity', 'Name','OperationId','OperationName','ParentId','UserId','SessionId', '_BilledSize','ItemCount','properties.recipientName', 'properties.locale','properties.activityType','properties.conversationId']

base_df = (silver_table
        .select(select_cols)
        .withColumn('date',F.to_date(F.col('time'))))

# COMMAND ----------

total_users = base_df.groupBy('date').agg(F.countDistinct('UserId').alias('totalUsers'))

client_type_distribution = (base_df
                            .groupBy('date','ClientType')
                            .agg(F.countDistinct('UserId').alias('userBase'))
                            .join(total_users ,'date','left' )
                            .withColumn('percentUsersCap', (F.col('userBase')/F.col('totalUsers'))*100))

user_prefered_channel = base_df.groupBy('date','UserId',"activityType").agg(F.sum('ItemCount').alias('interactions'), F.collect_set('AppRoleName').alias('communitcationChannel'))

# COMMAND ----------

# DBTITLE 1,CDF
output_table = DeltaTable.forPath(spark, f"{config['database_path']}/client_type_distribution")

(output_table
 .alias("t")
 .merge(client_type_distribution.alias("s"), 
        "t.date = s.date and t.ClientType = s.ClientType")
 .whenMatchedUpdate(set = 
                     {"t.userBase": "s.userBase",
                     "t.totalUsers": "s.totalUsers",
                     "t.percentUsersCap": "s.percentUsersCap"}
                    )
 .whenNotMatchedInsertAll()
 .execute())



# COMMAND ----------

spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).table("client_type_distribution").display()

# COMMAND ----------

# DBTITLE 1,dq + clone for archive
### convert the dataframe to a format compatible with Great Expectations
gdf = SparkDFDataset(user_prefered_channel.withColumn('date',F.col('date').cast('string')))

### start writing expectations we have about our data
gdf.expect_column_values_to_match_strftime_format(column='date',strftime_format='%Y-%m-%d')
gdf.expect_column_values_to_be_between("interactions", '0','9999999')

dbutils.fs.mkdirs(f"{config['expectation_suit_directory']}")
gdf.save_expectation_suite(f"{config['expectation_suit_directory']}/expectations.json")

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /dbfs/tmp/amitoz_sidhu/fs_demo/expectation_suit/expectations.json

# COMMAND ----------

gdf = SparkDFDataset(user_prefered_channel.withColumn('date',F.col('date').cast('string')))
gdf.validate(expectation_suite='/dbfs/tmp/amitoz_sidhu/fs_demo/expectation_suit/expectations.json')

# COMMAND ----------

(user_prefered_channel
 .write
 .format('delta')
 .mode('overwrite').option('mergeSchema','true')
#  .option('path',f"{config['database_path']}/user_prefered_channel")
 .saveAsTable(f"{config['database']}.user_prefered_channel")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE user_prefered_channel_archived  DEEP CLONE user_prefered_channel 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_prefered_channel_archived
