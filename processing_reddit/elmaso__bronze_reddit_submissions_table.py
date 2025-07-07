# Databricks notebook source
# DBTITLE 1,Imports

import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_utc_timestamp, lit, row_number, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, DateType, StringType 


# COMMAND ----------

# DBTITLE 1,Configs


# --- Get Job Parameters (replace with dbutils.widgets.get() for notebooks) ---
dbutils.widgets.text("ds", "", "Run Date (YYYY-MM-DD)")
run_date_str = datetime.date.today().strftime("%Y-%m-%d")
ds = dbutils.widgets.get("ds") if dbutils.widgets.get("ds") else run_date_str
source_table = "tabular.dataexpert.elmaso__raw_reddit_riot_submissions_async"
target_table = "tabular.dataexpert.elmaso__bronze_reddit_submissions_table" 

print(f"[{datetime.datetime.now()}] Starting Bronze Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

# DBTITLE 1,Load
raw_df = spark.table(source_table).filter(col("ds") == ds)
initial_count = raw_df.count()


# COMMAND ----------

# DBTITLE 1,dedup
# --- 2. De-duplicate and Clean (Bronze Logic) ---
window_spec = Window.partitionBy("submission_id").orderBy(col("_loaded_at_timestamp_utc").desc(), col("created_utc_unix").desc())


bronze_df = raw_df.withColumn("_row_num_rank", row_number().over(window_spec)) \
                    .filter(col("_row_num_rank") == 1) \
                    .select(
                        col("submission_id"),
                        col("author_name"),
                        col("created_utc").alias("raw_created_utc_str"), # Alias raw string column
                        col("created_utc_unix"),
                        to_utc_timestamp(from_unixtime(col("created_utc_unix")), lit('UTC')).alias("created_at_timestamp"), # Derive TIMESTAMP
                        col("edited").cast(StringType()), # Ensure type, raw is string as per DESCRIBE
                        col("flair").cast(StringType()),  # Ensure type
                        col("is_self"),
                        col("num_comments"),
                        col("over_18"),
                        col("permalink"),
                        col("retrieved_utc_unix"),
                        col("score"),
                        col("selftext"),
                        col("spoiler"),
                        col("stickied"),
                        col("subreddit").alias("raw_subreddit_name"), # Alias raw subreddit name
                        col("title"),
                        col("upvote_ratio"),
                        col("url"),
                        col("ds"), # Ingestion date partition
                        col("_loaded_at_timestamp_utc"),
                        # Remove _row_num_rank as it's not part of the final schema
                    )
                    
dedup_count = bronze_df.count()
print(f"deduplicated {initial_count - dedup_count} records out of {initial_count}.")

# COMMAND ----------

# DBTITLE 1,QA
 # --- 3. Apply Basic Data Quality Checks (PySpark Filtering/Assertions) ---
# These are conceptual and can be expanded. For strict validation, consider dbt tests or Great Expectations.

bronze_df_qc = bronze_df.filter(col("submission_id").isNotNull()) \
                        .filter(col("created_at_timestamp").isNotNull()) \
                        .filter(col("score") >= 0) # Basic positive score check

final_count = bronze_df_qc.count()
if dedup_count != final_count:
    print(f"[{datetime.datetime.now()}] WARNING: {dedup_count - final_count} records dropped due to QC.")
    # For a job, you might want to log this to a metrics table or send an alert

print(f"[{datetime.datetime.now()}] Processed {final_count} bronze records after de-duplication and QC.")
bronze_df_qc.limit(3).display()

# COMMAND ----------

# DBTITLE 1,Load

# --- 4. Write to Bronze Delta Table ---
try:
    # Ensure the table exists (can run CREATE TABLE IF NOT EXISTS via spark.sql)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            submission_id STRING,
            author_name STRING,
            raw_created_utc_str STRING,
            created_utc_unix BIGINT,
            created_at_timestamp TIMESTAMP,
            edited STRING,
            flair STRING,
            is_self BOOLEAN,
            num_comments BIGINT,
            over_18 BOOLEAN,
            permalink STRING,
            retrieved_utc_unix BIGINT,
            score BIGINT,
            selftext STRING,
            spoiler BOOLEAN,
            stickied BOOLEAN,
            raw_subreddit_name STRING,
            title STRING,
            upvote_ratio DOUBLE,
            url STRING,
            ds DATE,
            _loaded_at_timestamp_utc TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (ds);
    """)

    # Write, replacing only the current day's partition
    (bronze_df_qc.write
        .format("delta")
        .mode("overwrite").option("overwriteSchema", "true")  
    .option("replaceWhere", f"ds = '{ds}'") 
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Bronze submissions written successfully to {target_table}.")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to Bronze Delta table: {e}")
    sys.exit(1)

print(f"[{datetime.datetime.now()}] Bronze Submissions Batch Job completed.")