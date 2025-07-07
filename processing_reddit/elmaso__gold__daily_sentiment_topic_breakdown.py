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
target_table = "tabular.dataexpert.elmaso__gold__daily_sentiment_topic_breakdown" 

print(f"[{datetime.datetime.now()}] Starting Gold Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

gold__daily_sentiment_topic_breakdown = spark.sql(f"""
  SELECT  
      ds,
      text_category,
      ai_sentiment,
      COUNT(*) AS post_count,
      AVG(score) AS avg_score,
      COLLECT_LIST(DISTINCT ai_topics) AS ai_topic_samples
  FROM {source_table}
  WHERE ds = '{ds}'
  GROUP BY ds, text_category, ai_sentiment
    """)
gold__daily_sentiment_topic_breakdown.limit(3).display()


# COMMAND ----------

# DBTITLE 1,LOAD
try:
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {target_table} (
                ds DATE,
                text_category STRING,
                ai_sentiment STRING,
                post_count BIGINT,
                avg_score DOUBLE,
                ai_topic_samples ARRAY<STRING>
                )
                USING DELTA
                PARTITIONED BY (ds)
                
              """
    )
    # Write, replacing only the current day's partition
    (
        gold__daily_sentiment_topic_breakdown
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"ds = '{ds}'") 
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Successfully wrote {target_table} Delta table")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to {target_table}r Delta table: {e}")

 