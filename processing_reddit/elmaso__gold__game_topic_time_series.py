# Databricks notebook source
# DBTITLE 1,Imports
import sys
import datetime
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Configs
# --- Get Job Parameters (replace with dbutils.widgets.get() for notebooks) ---
dbutils.widgets.text("ds", "", "Run Date (YYYY-MM-DD)")
run_date_str = datetime.date.today().strftime("%Y-%m-%d")
ds = dbutils.widgets.get("ds") if dbutils.widgets.get("ds") else run_date_str
source_table = "tabular.dataexpert.elmaso__silver_reddit_submissions_cleaned"
target_table = "tabular.dataexpert.elmaso__gold__game_topic_time_series" 

print(f"[{datetime.datetime.now()}] Starting Gold Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

gold__game_topic_time_series = spark.sql(f"""
SELECT
  ds,
  game_topic,
  upvote_ratio_bin,  
  COUNT(*) AS submission_count,
  SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) AS trending_count,
  AVG(CASE WHEN spoiler THEN 1 ELSE 0 END) AS spoiler_rate,
  AVG(CASE WHEN over_18 THEN 1 ELSE 0 END) AS over_18_ratio
FROM {source_table}
WHERE ds = '{ds}'
GROUP BY ds, game_topic, upvote_ratio_bin
""")

gold__game_topic_time_series.limit(3).display()

# COMMAND ----------

# DBTITLE 1,LOAD
try:
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {target_table} (
                ds DATE,
                game_topic STRING,
                upvote_ratio_bin STRING,
                submission_count BIGINT,
                trending_count BIGINT,
                spoiler_rate DOUBLE,
                over_18_ratio DOUBLE
                )
                USING DELTA
                PARTITIONED BY (ds);
              """
    )
    # Write, replacing only the current day's partition
    (
        gold__game_topic_time_series
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"ds = '{ds}'") 
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Successfully wrote {target_table} Delta table")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to {target_table}r Delta table: {e}")

 