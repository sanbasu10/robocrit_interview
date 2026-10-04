# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog robocrit

# COMMAND ----------

display(spark.read.json('/Volumes/robocrit/robocrit_ingest_prod/landing/data_source_2/20250627/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from robocrit_bronze_prod.json_sensor_data where batch_id=2