# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text('env','prod')
dbutils.widgets.text('clear_data','1')

# COMMAND ----------

env=getArgument('env')
clear_data=getArgument('clear_data')


# COMMAND ----------

spark.sql(f'create schema if not exists robocrit.robocrit_gold_{env}')

# COMMAND ----------

if clear_data == "1":
    spark.sql(f"drop table if exists robocrit.robocrit_gold_{env}.combined_sensor_data")

# COMMAND ----------

spark.sql(f"""
create table if not exists robocrit.robocrit_gold_{env}.combined_sensor_data
(
event_date timestamp,
sensor_number int,
sensor_value decimal(20,16),
source_table string,
batch_id string,
load_date timestamp,
yyyymmdd string  
)
using delta 
partitioned by (yyyymmdd,batch_id,source_table)       
""")

# COMMAND ----------

def load_gold_table(file_type):
    display(spark.sql(f"""
    insert into robocrit.robocrit_gold_{env}.combined_sensor_data
            
    SELECT
    event_date,
    sensor_number,
    sensor_value,
    '{file_type}' as source_table,
    batch_id,load_date,yyyymmdd

    FROM robocrit.robocrit_silver_{env}.{file_type}_sensor_data
    where cast(batch_id as bigint) > (select coalesce(max(cast(batch_id as bigint)),0) from 
                        robocrit.robocrit_gold_{env}.combined_sensor_data
                        where source_table='{file_type}'
                        )
    """))

# COMMAND ----------

load_gold_table('csv')

# COMMAND ----------

load_gold_table('json')

# COMMAND ----------

# MAGIC %md
# MAGIC overall average of top 1/3 sensor value across all sensors

# COMMAND ----------

display(spark.sql(f"""select avg(sensor_value)  as avg_sensor_value from
(select sensor_number,sensor_value from
(SELECT
    sensor_number,
    sensor_value,
    event_date,
    yyyymmdd,
    PERCENT_RANK() OVER (
      PARTITION BY sensor_number
      ORDER BY sensor_value DESC
    ) AS pr
  FROM robocrit.robocrit_gold_{env}.combined_sensor_data)
  where pr <=(1/3)
  )
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC average of top 1/3 sensor value for each sensor

# COMMAND ----------

spark.sql(f"""create or replace view robocrit.robocrit_gold_{env}.avg_top_sensor_value_vw as 
select sensor_number ,avg(sensor_value) as avg_top_one_third_sensor_value from
(select sensor_number,sensor_value from
(SELECT
    sensor_number,
    sensor_value,
    event_date,
    yyyymmdd,
    PERCENT_RANK() OVER (
      PARTITION BY sensor_number
      ORDER BY sensor_value DESC
    ) AS pr
  FROM robocrit.robocrit_gold_{env}.combined_sensor_data)
  where pr <=(1/3)
  )
group by sensor_number""")

# COMMAND ----------

display(spark.sql(f"select * from avg_top_sensor_value_vw"))