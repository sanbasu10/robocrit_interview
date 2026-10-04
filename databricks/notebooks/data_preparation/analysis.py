# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog robocrit

# COMMAND ----------

# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog robocrit

# COMMAND ----------

tables=['robocrit_bronze_prod.csv_sensor_data','robocrit_bronze_prod.json_sensor_data','robocrit_silver_prod.csv_sensor_data','robocrit_silver_prod.json_sensor_data','robocrit_silver_prod.json_sensor_data_corrupted','robocrit_silver_prod.csv_sensor_data_corrupted','robocrit_gold_prod.combined_sensor_data']

# COMMAND ----------

for table in tables :
    try:
        print(table)
        display(spark.sql(f"SELECT * FROM {table} order by yyyymmdd desc , batch_id desc limit 10"))
    except Exception as e:
        print(e[0:200])