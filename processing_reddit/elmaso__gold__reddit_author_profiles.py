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
target_table = "tabular.dataexpert.elmaso__gold__reddit_author_profiles" 

print(f"[{datetime.datetime.now()}] Starting Gold Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

gold__reddit_author_profiles = spark.sql(f"""
SELECT
    author_name,
    COUNT(*) AS total_submissions,
    AVG(score) AS avg_score,
    AVG(num_comments) AS avg_num_comments,
    SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) AS trending_post_count,
    SUM(CASE WHEN ai_sentiment = 'positive' THEN 1 ELSE 0 END) / COUNT(*) AS positive_sentiment_ratio,
    COUNT(DISTINCT game_topic) AS topic_diversity,
    MIN(created_at_timestamp) AS first_seen,
    MAX(created_at_timestamp) AS last_seen
FROM {source_table}
WHERE author_name IS NOT NULL 
GROUP BY  author_name
    """)
gold__reddit_author_profiles.limit(5).display()


# COMMAND ----------

# DBTITLE 1,LOAD
try:
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {target_table} (
                author_name STRING,
                total_submissions BIGINT,
                avg_score DOUBLE,
                avg_num_comments DOUBLE,
                trending_post_count BIGINT,
                positive_sentiment_ratio DOUBLE,
                topic_diversity BIGINT,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP
                )
                USING DELTA;
              """
    )
    # Write, replacing only the current day's partition
    (
        gold__reddit_author_profiles
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Successfully wrote {target_table} Delta table")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to {target_table}r Delta table: {e}")

 

# COMMAND ----------

