# Databricks notebook source
# DBTITLE 1,Imports
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, length, lower, when

# COMMAND ----------

# DBTITLE 1,Configs
# --- Get Job Parameters (replace with dbutils.widgets.get() for notebooks) ---
dbutils.widgets.text("ds", "", "Run Date (YYYY-MM-DD)")
run_date_str = datetime.datetime.now()
ds = dbutils.widgets.get("ds") if dbutils.widgets.get("ds") else run_date_str
source_table = "tabular.dataexpert.elmaso__bronze_reddit_submissions_table"
target_table = "tabular.dataexpert.elmaso__silver_reddit_submissions_cleaned" # Your target Silver table

print(f"Run: {run_date_str} Starting Silver Submissions Batch Job for ds={ds}")
print(f"source table: {source_table}")
print(f"target table: {target_table}")

# COMMAND ----------

# DBTITLE 1,Extract
base_df = spark.table(source_table).filter(col("ds") == ds)
initial_count = base_df.count()

# COMMAND ----------

# DBTITLE 1,2. Apply Enrichments (Silver Logic)
silver_df = (base_df 
        .withColumn("text_content", concat_ws(' ', col("title"), col("selftext"))) 
        .withColumn("text_type", lit("submission"))
        .withColumn("score_bin",
            (
                when(col("score") >= 500, lit('500+'))
                .when(col("score") >= 100, lit('100-499'))
                .when(col("score") >= 10, lit('10-99'))
                .when(col("score") >= 1, lit('1-9'))
                .when(col("score") == 0, lit('0'))
                .otherwise(lit('Negative'))
            )
        ) \
        .withColumn("num_comments_bin",
            (
                when(col("num_comments") >= 100, lit('100+'))
                .when(col("num_comments") >= 50, lit('50-99'))
                .when(col("num_comments") >= 10, lit('10-49'))
                .when(col("num_comments") >= 1, lit('1-9'))
                .otherwise(lit('0'))
            )
        ) \
        .withColumn("upvote_ratio_bin",
            (
                when(col("upvote_ratio") >= 0.9, lit('Highly Upvoted (90%+)'))
                .when(col("upvote_ratio") >= 0.7, lit('Mostly Upvoted (70-89%)'))
                .when(col("upvote_ratio") >= 0.5, lit('Mixed (50-69%)'))
                .otherwise(lit('Mostly Downvoted (<50%)'))
            )
        ) \
        .withColumn("is_trending", (col("score") >= 5) & (col("num_comments") >= 4)) \
        .withColumn("text_category",
            (
                when(lower(col("title")).like('%patch%') | lower(col("selftext")).like('%patch%') | \
                     lower(col("title")).like('%update%') | lower(col("selftext")).like('%update%'), lit('Game Update/Patch'))
                .when(lower(col("title")).like('%esports%') | lower(col("selftext")).like('%esports%') | \
                      lower(col("title")).like('%pro play%') | lower(col("selftext")).like('%pro play%') | \
                      lower(col("title")).like('%tournament%') | lower(col("selftext")).like('%tournament%'), lit('Esports/Competitive'))
                # Trust & Safety
                .when(lower(col("title")).like('%hack%') | lower(col("selftext")).like('%hack%') | \
                      lower(col("title")).like('%cheat%') | lower(col("selftext")).like('%cheat%') | \
                      lower(col("title")).like('%exploit%') | lower(col("selftext")).like('%exploit%') | \
                      lower(col("title")).like('%bot%') | lower(col("selftext")).like('%bot%') | \
                      lower(col("title")).like('%toxicity%') | lower(col("selftext")).like('%toxicity%') | \
                      lower(col("title")).like('%abuse%') | lower(col("selftext")).like('%abuse%'), lit('Trust & Safety: Malicious Behavior'))
                .when(lower(col("title")).like('%bug%') | lower(col("selftext")).like('%bug%') | \
                      lower(col("title")).like('%crash%') | lower(col("selftext")).like('%crash%') | \
                      lower(col("title")).like('%performance%') | lower(col("selftext")).like('%performance%'), lit('Bug Report/Technical'))
                .when(lower(col("title")).like('%lore%') | lower(col("selftext")).like('%lore%') | \
                      lower(col("title")).like('%story%') | lower(col("selftext")).like('%story%'), lit('Lore/Story'))
                .when(lower(col("title")).like('%feedback%') | lower(col("selftext")).like('%feedback%') | \
                      lower(col("title")).like('%suggestion%') | lower(col("selftext")).like('%suggestion%'), lit('Player Feedback/Suggestions'))
                .when(lower(col("title")).like('%guide%') | lower(col("selftext")).like('%guide%') | \
                      lower(col("title")).like('%meta%') | lower(col("selftext")).like('%meta%'), lit('Gameplay Strategy/Tips'))
                .when(lower(col("title")).like('%fan art%') | lower(col("selftext")).like('%fan art%') | \
                      lower(col("title")).like('%meme%') | lower(col("selftext")).like('%meme%'), lit('Community/Social'))
                .when(lower(col("raw_subreddit_name")).isin('leagueoflegends', 'valorant', 'dota2', 'apexlegends', 'wildrift', 'teamfighttactics', 'legendsofruneterra'), lit('Game Specific Discussion'))
                .when(lower(col("raw_subreddit_name")).isin('gaming', 'pcgaming', 'consolegaming', 'games'), lit('General Gaming Discussion'))
                .otherwise(lit('Other'))
            )
        ) \
        .withColumn("game_topic",
            (
                when(lower(col("raw_subreddit_name")) == 'leagueoflegends', lit('League of Legends'))
                .when(lower(col("raw_subreddit_name")) == 'valorant', lit('Valorant'))
                .when(lower(col("raw_subreddit_name")) == 'teamfighttactics', lit('Teamfight Tactics'))
                .when(lower(col("raw_subreddit_name")) == 'legendsofruneterra', lit('Legends of Runeterra'))
                .when(lower(col("raw_subreddit_name")) == 'wildrift', lit('Wild Rift'))
                .when(lower(col("raw_subreddit_name")) == 'dota2', lit('DotA 2'))
                .when(lower(col("raw_subreddit_name")) == 'globaloffensive', lit('CS:GO'))
                .when(lower(col("raw_subreddit_name")) == 'apexlegends', lit('Apex Legends'))
                .when(lower(col("raw_subreddit_name")) == 'heroesofthestorm', lit('Heroes of the Storm'))
                .otherwise(lit('Other Game'))
            )
        )\
        .withColumn("is_ai_enrichment_candidate", # Flag for filtering for AI costs
            (
                (col("is_trending") == True) | # Trending content
                (length(col("text_content")) > 500)  # Long text content
            )
        )
)
        
# Register the DataFrame as a temporary view
silver_df.createOrReplaceTempView("silver_df")

# COMMAND ----------

# DBTITLE 1,Basi AI
silver_ai_df = spark.sql("""
    select 
    -- Identifiers
    submission_id as content_id,
    text_type as content_type,
    -- Core Meta
    author_name,
    raw_subreddit_name as content_subreddit,

    -- Text cotent
    title as  text_title,
    selftext as text_content,
    url as text_url,

    -- Categorization as Game options
    text_category,
    game_topic,
    flair as class_type,
    not game_topic = 'Other Game' as is_game_topic,

    -- AI ENRICHMENT

    ai_analyze_sentiment(text_content) as ai_sentiment,
    ai_classify(text_content, array('Game Update/Patch','Bug Report/Technical','Esports/Competitive','Lore/Story','Player Bots/Detected','Player Feedback/Suggestions','Gameplay Strategy/Tip','Community/Social','Other')) as ai_topics,
    not edited = 'False' as edited,
   
    -- --- BEHAVIORAL METRICS & BINS ---
    
    is_trending,
    upvote_ratio_bin,
    num_comments_bin,
    score_bin,
    is_self,
    over_18,
    num_comments,
    score,
    spoiler,
        -- time partitios
    created_at_timestamp,
    _loaded_at_timestamp_utc,
    ds
    from `silver_df`
    where is_ai_enrichment_candidate
"""
)

# COMMAND ----------

# DBTITLE 1,Load
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            content_id STRING,
            content_type STRING NOT NULL,
            author_name STRING,
            content_subreddit STRING,
            text_title STRING,
            text_content STRING,
            text_url STRING,
            text_category STRING NOT NULL,
            game_topic STRING NOT NULL,
            class_type STRING,
            is_game_topic BOOLEAN NOT NULL,
            ai_sentiment STRING,
            ai_topics STRING,
            edited BOOLEAN,
            is_trending BOOLEAN,
            upvote_ratio_bin STRING NOT NULL,
            num_comments_bin STRING NOT NULL,
            score_bin STRING NOT NULL,
            is_self BOOLEAN,
            over_18 BOOLEAN,
            num_comments BIGINT,
            score BIGINT,
            spoiler BOOLEAN,
            created_at_timestamp TIMESTAMP,
            _loaded_at_timestamp_utc TIMESTAMP,
            ds DATE
            )
            USING DELTA
            PARTITIONED BY (ds)
            TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true' -- optional: enables CDC if you need it
            )
        COMMENT 'Silver table for cleaned Reddit submissions with AI enrichment and categorization'
        """
    )
    # Write, replacing only the current day's partition
    (silver_ai_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") 
        .option("replaceWhere", f"ds = '{ds}'") 
        .saveAsTable(target_table))
    print(f"[{datetime.datetime.now()}] Siver table for cleaned Reddit submissions written successfully to {target_table}.")
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error writing to Silver Delta table: {e}")

    