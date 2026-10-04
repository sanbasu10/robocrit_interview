# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text('env','prod')
# dbutils.widgets.text('table','csv_sensor_data')
# dbutils.widgets.text('table','json_sensor_data')
# dbutils.widgets.text('clear_data','1')


# COMMAND ----------

env=getArgument('env')
table=getArgument('table')
clear_data=getArgument('clear_data')

# COMMAND ----------

spark.sql(f'create schema if not exists robocrit.robocrit_silver_{env}')

# COMMAND ----------

table

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""
create table if not exists robocrit.robocrit_silver_{env}.{table}
(
event_date timestamp,
sensor_number int,
sensor_value decimal(20,16),
batch_id string,
load_date timestamp,
yyyymmdd string  
)
using delta 
partitioned by (yyyymmdd,batch_id)       
""")

# COMMAND ----------

spark.sql(f"""
create table if not exists robocrit.robocrit_silver_{env}.{table}_corrupted
(
event_date string,
sensor_number string,
sensor_value string,
batch_id string,
load_date timestamp,
yyyymmdd string  
)
using delta 
partitioned by (yyyymmdd,batch_id)       
""")

# COMMAND ----------

display(spark.sql(f"""
insert into robocrit.robocrit_silver_{env}.{table}  
select * from              
(SELECT
  COALESCE(
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm:ss.SSS'),
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm:ss'),
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm'),
    try_to_timestamp(event_date)
  ) AS event_date,
  try_cast(sensor_number as int) as sensor_number,
  try_cast(sensor_value as decimal(20,16)) as sensor_value,
  batch_id,load_date,yyyymmdd

FROM robocrit.robocrit_bronze_{env}.{table} 
where batch_id > (select coalesce(max(batch_id),0) from robocrit.robocrit_silver_{env}.{table})
)
where sensor_number is not null
and sensor_value is not null
and event_date is not null

"""))

# COMMAND ----------

display(spark.sql(f"""
insert into robocrit.robocrit_silver_{env}.{table}_corrupted
select event_date,sensor_number,sensor_value,batch_id,load_date,yyyymmdd from              
(SELECT
    event_date,sensor_number,sensor_value,
    COALESCE(
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm:ss.SSS'),
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm:ss'),
    try_to_timestamp(event_date, 'yyyy-MM-dd HH:mm'),
    try_to_timestamp(event_date)
    ) AS clean_event_date,
    try_cast(sensor_number as int) as clean_sensor_number,
    try_cast(sensor_value as decimal(20,16)) as clean_sensor_value,
    batch_id,load_date,yyyymmdd

FROM robocrit.robocrit_bronze_{env}.{table} 
where batch_id > (select coalesce(max(batch_id),0) from robocrit.robocrit_silver_{env}.{table}_corrupted )
)
where clean_sensor_number is  null
or clean_sensor_value is  null
or clean_event_date is  null

"""))

# COMMAND ----------

display(spark.sql(f"""select * from robocrit.robocrit_silver_{env}.{table} limit 10"""))

# COMMAND ----------

display(spark.sql(f"""select * from robocrit.robocrit_silver_{env}.{table}_corrupted limit 10"""))

# COMMAND ----------

display(spark.sql(f"select count(1),batch_id,yyyymmdd from robocrit.robocrit_silver_{env}.{table} group by yyyymmdd,batch_id order by yyyymmdd,batch_id"))

# COMMAND ----------

display(spark.sql(f"select count(1),batch_id,yyyymmdd from robocrit.robocrit_silver_{env}.{table}_corrupted group by yyyymmdd,batch_id order by yyyymmdd,batch_id"))