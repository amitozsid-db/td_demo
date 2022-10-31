# Databricks notebook source
# MAGIC %run ./00a-setup

# COMMAND ----------

spark.sql(f""" OPTIMIZE  delta.`{config['database_path']+'/silver_table'}` ZORDER BY time """)
