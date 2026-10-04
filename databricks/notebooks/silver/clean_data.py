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

if clear_data == "1":
  spark.sql(f"drop table if exists robocrit.robocrit_silver_{env}.{table}")
  spark.sql(f"drop table if exists robocrit.robocrit_silver_{env}.{table}_corrupted")

# COMMAND ----------

table

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
yyyymmdd string,
rescued_data string,
corruption_reason string
)
using delta 
partitioned by (yyyymmdd,batch_id)       
""")

# COMMAND ----------

corrupted_table = f"robocrit.robocrit_silver_{env}.{table}_corrupted"
corrupted_columns = {column.lower() for column in spark.table(corrupted_table).columns}

for column in ("rescued_data", "corruption_reason"):
  if column not in corrupted_columns:
    spark.sql(f"ALTER TABLE {corrupted_table} ADD COLUMNS ({column} STRING)")

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
where cast(batch_id as bigint) > (select coalesce(max(cast(batch_id as bigint)),0) from robocrit.robocrit_silver_{env}.{table})
and _rescued_data is null
and try_to_timestamp(string(yyyymmdd), 'yyyyMMdd') is not null
)
where sensor_number is not null
and sensor_value is not null
and event_date is not null

"""))

# COMMAND ----------

display(spark.sql(f"""
insert into robocrit.robocrit_silver_{env}.{table}_corrupted
  (event_date, sensor_number, sensor_value, batch_id, load_date, yyyymmdd, rescued_data, corruption_reason)
select
  event_date,
  sensor_number,
  sensor_value,
  batch_id,
  load_date,
  yyyymmdd,
  _rescued_data as rescued_data,
  concat_ws('; ',
    case when clean_event_date is null then 'invalid_event_date' end,
    case when clean_sensor_number is null then 'invalid_sensor_number' end,
    case when clean_sensor_value is null then 'invalid_sensor_value' end,
    case when _rescued_data is not null then 'schema_rescue' end,
    case when try_to_timestamp(string(yyyymmdd), 'yyyyMMdd') is null then 'invalid_source_folder_date' end
  ) as corruption_reason
from              
(SELECT
    event_date,sensor_number,sensor_value,_rescued_data,
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
where cast(batch_id as bigint) > (select coalesce(max(cast(batch_id as bigint)),0) from robocrit.robocrit_silver_{env}.{table}_corrupted )
)
where clean_sensor_number is  null
or clean_sensor_value is  null
or clean_event_date is  null
or _rescued_data is not null
or try_to_timestamp(string(yyyymmdd), 'yyyyMMdd') is null

"""))

# COMMAND ----------

display(spark.sql(f"""select * from robocrit.robocrit_silver_{env}.{table} limit 10"""))

# COMMAND ----------

display(spark.sql(f"""select * from robocrit.robocrit_silver_{env}.{table}_corrupted limit 10"""))