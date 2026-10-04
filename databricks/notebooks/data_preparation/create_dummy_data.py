# Databricks notebook source
clear_data='1'
if clear_data=='1':
    env='prod'
    dbutils.fs.rm('/Volumes/robocrit/robocrit_bronze_prod/checkpoints/csv_sensor_data',True)
    dbutils.fs.rm('/Volumes/robocrit/robocrit_bronze_prod/checkpoints/json_sensor_data',True)
    dbutils.fs.rm('/Volumes/robocrit/robocrit_bronze_prod/schemapaths/csv_sensor_data',True)
    dbutils.fs.rm('/Volumes/robocrit/robocrit_bronze_prod/schemapaths/json_sensor_data',True)
    tables=['robocrit_bronze_prod.csv_sensor_data','robocrit_bronze_prod.json_sensor_data','robocrit_silver_prod.csv_sensor_data','robocrit_silver_prod.json_sensor_data','robocrit_silver_prod.json_sensor_data_corrupted','robocrit_silver_prod.csv_sensor_data_corrupted','robocrit_gold_prod.combined_sensor_data']
    for table in tables:
        display(spark.sql(f"delete from robocrit.{table}"))
        print(f"Deleted table {table}")
    print("Deleted all tables")

# COMMAND ----------

files=dbutils.fs.ls('/Volumes/robocrit/robocrit_ingest_prod/landing/data_source_1')
for d in files :
    if d.name!='20250627/':
        print(d.path)
        dbutils.fs.rm(d.path,True)

# COMMAND ----------

files=dbutils.fs.ls('/Volumes/robocrit/robocrit_ingest_prod/landing/data_source_2')
for d in files :
    if d.name!='20250627/':
        print(d.path)
        dbutils.fs.rm(d.path,True)

# COMMAND ----------

date_partitions = ["20260102","20260103","20260104","20260105","20260106","20260107","20260108","20260109","20260110",]
min_files = 10
max_files = 15
min_rows = 1000000
max_rows = 2000000

# COMMAND ----------

# Databricks Notebook: Generate Sensor Data CSV Files (FIXED)
# Creates actual .csv files, not directories

import random
from datetime import datetime, timedelta
import csv
import io

# ============================================================================
# CONFIGURATION
# ============================================================================

base_path = "/Volumes/robocrit/robocrit_ingest_prod/landing/data_source_1"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_event_date():
    """Generate random event_date in various formats"""
    base = datetime(2025, 6, 20, 15, 37, 42)
    offset = timedelta(
        days=random.randint(0, 10),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        microseconds=random.randint(0, 999999)
    )
    dt = base + offset
    
    format_choice = random.choice([
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M'
    ])
    
    return dt.strftime(format_choice)


def generate_row():
    """Generate a single data row"""
    sensor_number = random.randint(0, 100)
    return {
        'event_date': generate_event_date(),
        'sensor_number': sensor_number,
        'sensor_value': round(random.uniform(0, 100), random.randint(10, 15))
    }


def write_csv_to_volume(file_path, data, batch_size=10000):
    """
    Write data to CSV file in Databricks Volume using dbutils.fs.put
    Writes in batches to avoid memory issues with large files
    """
    # Create CSV content in memory (for smaller files) or stream (for larger)
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['event_date', 'sensor_number', 'sensor_value'])
    
    # Write all rows
    for row in data:
        writer.writerow([
            row['event_date'],
            row['sensor_number'],
            row['sensor_value']
        ])
    
    # Get CSV content
    csv_content = output.getvalue()
    output.close()
    
    # Write to volume using dbutils (this creates an actual file, not a directory)
    dbutils.fs.put(file_path, csv_content, overwrite=True)
    
    return len(data)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 80)
print("Starting CSV File Generation")
print("=" * 80)

for partition_date in date_partitions:
    print(f"\n{'='*80}")
    print(f"Processing partition: {partition_date}")
    print(f"{'='*80}")
    
    partition_path = f"{base_path}/{partition_date}"
    
    # Ensure partition directory exists
    try:
        dbutils.fs.mkdirs(partition_path)
        print(f"Directory created/verified: {partition_path}/")
    except Exception as e:
        print(f"Directory may already exist: {e}")
    
    num_files = random.randint(min_files, max_files)
    print(f"Number of files to create: {num_files}")
    
    for file_idx in range(1, num_files + 1):
        num_rows = random.randint(min_rows, max_rows)
        file_name = f"sensor_data_{file_idx:02d}.csv"
        file_path = f"{partition_path}/{file_name}"
        
        print(f"  File {file_idx}/{num_files}: Generating {num_rows:,} rows...", end=" ")
        
        # Generate data
        data = [generate_row() for _ in range(num_rows)]
        
        # Write to file
        rows_written = write_csv_to_volume(file_path, data)
        
        print(f"✓ Written {rows_written:,} rows to {file_path}")

print("\n" + "=" * 80)
print("CSV File Generation Complete!")
print("=" * 80)

# ============================================================================
# VERIFICATION
# ============================================================================

print("\n" + "=" * 80)
print("Verification: Listing all created files")
print("=" * 80)

for partition_date in date_partitions:
    partition_path = f"{base_path}/{partition_date}"
    print(f"\n{partition_path}/")
    try:
        files = dbutils.fs.ls(partition_path)
        for file_info in files:
            file_name = file_info.name
            file_size = file_info.size
            print(f"  📄 {file_name} ({file_size:,} bytes)")
    except Exception as e:
        print(f"  Error listing files: {e}")

# COMMAND ----------

# Databricks Notebook: Generate Sensor Data JSON Files
# Creates JSON files with list of dictionaries

import random
from datetime import datetime, timedelta
import json

# ============================================================================
# CONFIGURATION
# ============================================================================

base_path = "/Volumes/robocrit/robocrit_ingest_prod/landing/data_source_2"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_event_date():
    """Generate random event_date in various formats"""
    base = datetime(2025, 6, 20, 15, 37, 42)
    offset = timedelta(
        days=random.randint(0, 10),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        microseconds=random.randint(0, 999999)
    )
    dt = base + offset
    
    format_choice = random.choice([
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M'
    ])
    
    return dt.strftime(format_choice)


def generate_row():
    """Generate a single data row as dictionary"""
    sensor_number = random.randint(0, 100)
    return {
        'event_date': generate_event_date(),
        'sensor_number': sensor_number,
        'sensor_value': round(random.uniform(0, 100), random.randint(10, 15))
    }


def write_json_to_volume(file_path, data):
    """
    Write data to JSON file in Databricks Volume.
    Each dictionary is on its own line, with compact JSON formatting.
    """
    json_content = "[\n" + ",\n".join(
        json.dumps(row, separators=(",", ":"))
        for row in data
    ) + "\n]"

    dbutils.fs.put(file_path, json_content, overwrite=True)

    return len(data)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 80)
print("Starting JSON File Generation")
print("=" * 80)

for partition_date in date_partitions:
    print(f"\n{'='*80}")
    print(f"Processing partition: {partition_date}")
    print(f"{'='*80}")
    
    partition_path = f"{base_path}/{partition_date}"
    
    # Ensure partition directory exists
    try:
        dbutils.fs.mkdirs(partition_path)
        print(f"Directory created/verified: {partition_path}/")
    except Exception as e:
        print(f"Directory may already exist: {e}")
    
    num_files = random.randint(min_files, max_files)
    print(f"Number of files to create: {num_files}")
    
    for file_idx in range(1, num_files + 1):
        num_rows = random.randint(min_rows, max_rows)
        file_name = f"sensor_data_{file_idx:02d}.json"
        file_path = f"{partition_path}/{file_name}"
        
        print(f"  File {file_idx}/{num_files}: Generating {num_rows:,} rows...", end=" ")
        
        # Generate data
        data = [generate_row() for _ in range(num_rows)]
        
        # Write to file
        rows_written = write_json_to_volume(file_path, data)
        
        print(f"✓ Written {rows_written:,} rows to {file_path}")

print("\n" + "=" * 80)
print("JSON File Generation Complete!")
print("=" * 80)

# ============================================================================
# VERIFICATION
# ============================================================================

print("\n" + "=" * 80)
print("Verification: Listing all created files")
print("=" * 80)

for partition_date in date_partitions:
    partition_path = f"{base_path}/{partition_date}"
    print(f"\n{partition_path}/")
    try:
        files = dbutils.fs.ls(partition_path)
        for file_info in files:
            file_name = file_info.name
            file_size = file_info.size
            print(f"  📄 {file_name} ({file_size:,} bytes)")
    except Exception as e:
        print(f"  Error listing files: {e}")