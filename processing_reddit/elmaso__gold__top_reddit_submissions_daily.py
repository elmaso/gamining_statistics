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
target_table = "tabular.dataexpert.elmaso__gold__top_reddit_submissions_dailys" 

print(f"[{datetime.datetime.now()}] Starting Gold Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

gold__top_reddit_submissions_dailys = spark.sql(f"""
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ds ORDER BY score DESC) AS rank_score,
         ROW_NUMBER() OVER (PARTITION BY ds ORDER BY num_comments DESC) AS rank_comments
  FROM {source_table}
  WHERE ds = '{ds}'
)
SELECT ds as ds,
       content_id,
       author_name,
       game_topic,
       score,
       num_comments,
       text_title,
       text_content,
       text_url,
       ai_sentiment,
       ai_topics,
       rank_score,
       rank_comments
FROM ranked
WHERE rank_score <= 10 OR rank_comments <= 10
    """)
gold__top_reddit_submissions_dailys.display()


# COMMAND ----------

# DBTITLE 1,LOAD
try:
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {target_table} (
                ds DATE,
                content_id STRING,
                author_name STRING,
                game_topic STRING,
                score BIGINT,
                num_comments BIGINT,
                text_title STRING,
                text_content STRING,
                text_url STRING,
                ai_sentiment STRING,
                ai_topics STRING,
                rank_score INT,
                rank_comments INT
                )
                USING DELTA
                PARTITIONED BY (ds)
                
              """
    )
    # Write, replacing only the current day's partition
    (
        gold__top_reddit_submissions_dailys
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"ds = '{ds}'") 
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Successfully wrote {target_table} Delta table")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to {target_table}r Delta table: {e}")

 