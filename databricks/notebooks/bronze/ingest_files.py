# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# dbutils.widgets.text('env','prod')

# dbutils.widgets.text('schema_path',"/Volumes/robocrit/robocrit_bronze_{env}/schemapaths")
# dbutils.widgets.text('checkpoint_path',"/Volumes/robocrit/robocrit_bronze_{env}/checkpoints")
# dbutils.widgets.text('clear_data','1')

# dbutils.widgets.text('file_type','csv')
# dbutils.widgets.text('file_path','/Volumes/robocrit/robocrit_ingest_{env}/landing/data_source_1/')

# dbutils.widgets.text('file_type','json')
# dbutils.widgets.text('file_path','/Volumes/robocrit/robocrit_ingest_{env}/landing/data_source_2/')

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch all job parameters

# COMMAND ----------

env=getArgument('env')
file_path=getArgument('file_path').format(env=env)
schema_path=getArgument('schema_path').format(env=env)
checkpoint_path=getArgument('checkpoint_path').format(env=env)
file_type=getArgument('file_type')
target_table=f"{file_type}_sensor_data"
full_table = f"robocrit.robocrit_bronze_{env}.{target_table}"
clear_data=getArgument('clear_data')

print(f"env: {env}")
print(f"file_path: {file_path}")
print(f"schema_path: {schema_path}")
print(f"checkpoint_path: {checkpoint_path}")
print(f"file_type: {file_type}")
print(f"target_table: {target_table}")
print(f"full_table: {full_table}")
print(f"clear_data: {clear_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Schema

# COMMAND ----------

spark.sql(f'create schema if not exists robocrit.robocrit_bronze_{env}')

# COMMAND ----------

# MAGIC %md
# MAGIC Create Unity Catalog Volume for Shema Location and Checkpoints

# COMMAND ----------

spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS robocrit.robocrit_bronze_{env}.schemapaths
  COMMENT 'Auto Loader schema paths for bronze ingestion'
""")

spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS robocrit.robocrit_bronze_{env}.checkpoints
  COMMENT 'Streaming checkpoints for bronze ingestion'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC Creating Table Specific Schema Location within the Main Schema Path

# COMMAND ----------

dbutils.fs.ls(checkpoint_path)

# COMMAND ----------

dbutils.fs.ls(schema_path)

# COMMAND ----------

dbutils.fs.ls(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC add_batch_and_partition Function:
# MAGIC 1. yyyymmdd : partition column, value fetched from the source date folder
# MAGIC 2. load_date : Load timestamp
# MAGIC 3. batch_id : got it from epoch id , incremented by 1 
# MAGIC
# MAGIC Validations pre write :
# MAGIC 1. Making sure all source sub folders are in yyyymmdd format to make sure correct partition column values
# MAGIC 2. Making sure there are no corrupt records in the data

# COMMAND ----------


def add_batch_and_partition(df, epoch_id ):
    # global max_batch_id
    batch_id = epoch_id + 1
    df = (df.withColumn("yyyymmdd", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)/[^/]+$", 1))
          .withColumn("load_date", F.current_timestamp())
          .withColumn("batch_id", F.lit(batch_id))
         )

    (df.write.format("delta")
       .mode("append")
       .partitionBy("yyyymmdd", "batch_id")
       .option("optimizeWrite", "true")
       .saveAsTable(full_table))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Using auto loader
# MAGIC 2. Fetching file type from input 
# MAGIC 3. Writing schema into schema location
# MAGIC 4. Fetching max 1000 files per batch
# MAGIC 5. Fetching max 50GB data per batch 

# COMMAND ----------

df_raw = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", file_type)
          .option("cloudFiles.schemaLocation", f"{schema_path}/{target_table}")
          .option("cloudFiles.schemaEvolutionMode", "rescue")
          .option("rescuedDataColumn", "_rescued_data")
        #   .option("cloudFiles.useManagedFileEvents", "true")
          .option("cloudFiles.maxFilesPerTrigger", "1000")     # tune based on cluster
          .option("cloudFiles.maxBytesPerTrigger", "50g")      # ~50 GB per micro-batch
          .option("header", "true")
          .option("inferSchema", "true")
          .load(file_path))

query = (df_raw
         .writeStream
         .foreachBatch(add_batch_and_partition)
         .trigger(availableNow=True)
         .option("checkpointLocation", f"{checkpoint_path}/{target_table}")
         .start())

query.awaitTermination()

# COMMAND ----------

full_table

# COMMAND ----------
spark.sql(f"optimize table {full_table}")
display(spark.sql(f"select * from {full_table} limit 10"))