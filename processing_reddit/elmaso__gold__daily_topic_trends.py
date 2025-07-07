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
target_table = "tabular.dataexpert.elmaso__gold__daily_topic_trends" 

print(f"[{datetime.datetime.now()}] Starting Gold Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

gold__daily_topic_trends = spark.sql(f"""
SELECT
    ds,
    game_topic,
    COUNT(*) AS submission_count,
    AVG(score) AS avg_score,
    AVG(num_comments) AS avg_num_comments,
    AVG(CASE WHEN is_trending THEN 1 ELSE 0 END) AS trending_ratio
FROM {source_table}
WHERE game_topic IS NOT NULL and ds = '{ds}'
GROUP BY  ds, game_topic
    """)
gold__daily_topic_trends.limit(5).display()


# COMMAND ----------

# DBTITLE 1,LOAD
try:
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {target_table} (
                ds DATE,
                game_topic STRING,
                submission_count BIGINT,
                avg_score DOUBLE,
                avg_num_comments DOUBLE,
                trending_ratio DOUBLE
                )
                USING DELTA
                PARTITIONED BY (ds)
                
              """
    )
    # Write, replacing only the current day's partition
    (
        gold__daily_topic_trends
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"ds = '{ds}'") 
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Successfully wrote {target_table} Delta table")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to {target_table}r Delta table: {e}")

 

# COMMAND ----------

