# Databricks notebook source
# DBTITLE 1,dep installs
# MAGIC %pip install asyncpraw asyncprawcore nest_asyncio
# MAGIC %restart_python 

# COMMAND ----------

# DBTITLE 1,imports
import asyncpraw
import asyncio
import nest_asyncio
import time
import random
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T


nest_asyncio.apply()

# COMMAND ----------

# DBTITLE 1,configs
dbutils.widgets.text("ds", "", "Run Date (YYYY-MM-DD)")
# dbutils.widgets.text("output_submissions_table", "tabular.dataexpert.elmaso__reddit_riot_submissions_raw_delta", "Output Table Name")
# dbutils.widgets.text("output_comments_table", "tabular.dataexpert.elmaso__reddit_riot_comments_raw_delta", "Output Table Name")
dbutils.widgets.text("PRAW_CLIENT_ID", "K0jhjnZmf53sndwQmm50ig", "Reddit Client ID")
dbutils.widgets.text("PRAW_CLIENT_SECRET", "3aF6Kce2BGmy2-FK0cR5hv5qsTIzNA", "Reddit Client Secret")
dbutils.widgets.text("PRAW_USER_AGENT", "MyCapstoneBot/0.1 by elmaso", "Reddit User Agent")
dbutils.widgets.text("TARGET_SUBREDDITS_LIST", "leagueoflegends,valorant", "RiotGames")
dbutils.widgets.text("SUBMISSION_FETCH_LIMIT", "50", "Submission Fetch Limit")
dbutils.widgets.text("COMMENT_FETCH_LIMIT", "50", "Comment Fetch Limit")

# COMMAND ----------

try:
    if not dbutils.widgets.get("ds") == "":
        print("NOTEBOOK JOB CONFIGS: (Using widgets params)")
    # Praw Config
        PRAW_CLIENT_ID = dbutils.widgets.get("PRAW_CLIENT_ID")
        PRAW_CLIENT_SECRET = dbutils.widgets.get("PRAW_CLIENT_SECRET")
        PRAW_USER_AGENT = dbutils.widgets.get("PRAW_USER_AGENT")
    # Config scraping params
        TARGET_SUBREDDITS_LIST = dbutils.widgets.get("TARGET_SUBREDDITS_LIST").split(",")
        SUBMISSION_FETCH_LIMIT = int(dbutils.widgets.get("SUBMISSION_FETCH_LIMIT"))
        COMMENT_FETCH_LIMIT = int(dbutils.widgets.get("COMMENT_FETCH_LIMIT"))
        
    # Config raw output tables:
        ds = dbutils.widgets.get("ds")
        
except:
    print("Will init from io job config:") #-- raise Exception("Please provide a date in the format YYYY-MM-DD")
    # TARGET_SUBREDDITS_LIST = spark.conf.get("spark.databricks.io.TARGET_SUBREDDITS_LIST","riot,lol").split(",")
    # SUBMISSION_FETCH_LIMIT = int(spark.conf.get("spark.databricks.io.SUBMISSION_FETCH_LIMIT", "50"))
    # COMMENT_FETCH_LIMIT = int(spark.conf.get("spark.databricks.io.COMMENT_FETCH_LIMIT", "50"))


output_submissions_table = "tabular.dataexpert.elmaso__raw_reddit_riot_submissions_async"
output_comments_table = "tabular.dataexpert.elmaso__raw_reddit_riot_comments_async"

# Praw Config should be set in secrets dont have PAT
PRAW_CLIENT_ID = 'K0jhjnZmf53sndwQmm50ig'
PRAW_CLIENT_SECRET = '3aF6Kce2BGmy2-FK0cR5hv5qsTIzNA'
PRAW_USER_AGENT = 'MyCapstoneBot/0.1 by elmaso'


print(f"Scraping params: run_date {ds}")
print(f" -TARGET_SUBREDDITS_LIST: {TARGET_SUBREDDITS_LIST}")
print(f" -SUBMISSION_FETCH_LIMIT: {SUBMISSION_FETCH_LIMIT}")
print(f" -COMMENT_FETCH_LIMIT: {COMMENT_FETCH_LIMIT}")
print(f"Output Tables:")
print(f" -Submissions Table: {output_submissions_table}")
print(f" -Comments Table: {output_comments_table}")


# COMMAND ----------

# DBTITLE 1,Init Reddit Client
reddit = asyncpraw.Reddit(
    client_id=PRAW_CLIENT_ID,
    client_secret=PRAW_CLIENT_SECRET,
    user_agent=PRAW_USER_AGENT
)

# COMMAND ----------

# DBTITLE 1,Praw Scrapping Async
# Async function to fetch submissions and comments
async def fetch_submission_and_comments(subreddit_name, submission_limit=5, comment_limit_per_submission=None):
    subreddit = await reddit.subreddit(subreddit_name)
    submission_rows = []
    comment_rows = []

    try:
        subreddit = await reddit.subreddit(subreddit_name)
        try:
            submissions = subreddit.new(limit=submission_limit)
        except Exception as e:
            print(f"[WARNING] .new() failed for r/{subreddit_name}, switching to .top(): {e}")
            submissions = subreddit.top(limit=submission_limit)

        async for submission in submissions:
            try:
                await submission.load()
                await asyncio.sleep(random.uniform(1.5, 3.0))  # random sleep to avoid rate limit

                submission_rows.append({
                    "subreddit": subreddit.display_name,
                    "submission_id": submission.id,
                    "title": submission.title,
                    "selftext": getattr(submission, "selftext", ""),
                    "author_name": str(submission.author) if submission.author else "[deleted]",
                    "score": int(submission.score) if submission.score is not None else 0,
                    "upvote_ratio": float(getattr(submission, "upvote_ratio", 0.0)),
                    "num_comments": int(submission.num_comments) if submission.num_comments is not None else 0,
                    "created_utc": datetime.utcfromtimestamp(submission.created_utc).isoformat(),
                    "created_utc_unix": int(submission.created_utc),
                    "edited": str(getattr(submission, "edited", "false")),
                    "stickied": bool(getattr(submission, "stickied", False)),
                    "over_18": bool(getattr(submission, "over_18", False)),
                    "is_self": bool(getattr(submission, "is_self", False)),
                    "spoiler": bool(getattr(submission, "spoiler", False)),
                    "flair": str(getattr(submission, "link_flair_text", "")),
                    "url": submission.url,
                    "permalink": str(getattr(submission, "permalink", "")),
                    "retrieved_utc_unix": int(time.time()) # When this record was fetched by your job
                })

                try:
                    await submission.comments.replace_more(limit=0)
                except Exception as e:
                    print(f"Comment loading error for submission {submission.id}: {e}")
                    continue

                comment_count = 0
                for comment in submission.comments.list():
                    if comment_limit_per_submission and comment_count >= comment_limit_per_submission:
                        break

                    comment_rows.append({
                        "submission_id": submission.id,
                        "comment_id": comment.id,
                        "parent_id": str(getattr(comment, "parent_id", "")),
                        "body": comment.body,
                        "author_name": str(comment.author) if comment.author else "[deleted]",
                        "score": int(comment.score) if comment.score is not None else 0,
                        "created_utc": datetime.utcfromtimestamp(comment.created_utc).isoformat(),
                        "created_utc_unix": int(comment.created_utc),
                        "is_submitter": bool(getattr(comment, "is_submitter", False)),
                        "stickied": bool(getattr(comment, "stickied", False)),
                        "edited": str(getattr(comment, "edited", "false")),
                        "permalink": str(getattr(comment, "permalink", "")),
                        "retrieved_utc_unix": int(time.time()) # When this record was fetched by your job
                    })
                    comment_count += 1
    
            except Exception as e:
                print(f"[ERROR] Failed to process submission in r/{subreddit_name}: {e}")
                continue

    except Exception as e:
        print(f"[error] Could not access subreddit r/{subreddit_name}: {e}")
        return [],[]

    return submission_rows, comment_rows

# COMMAND ----------

# DBTITLE 1,Submissions and comments scraping
async def praw_scrape(subreddit_list=TARGET_SUBREDDITS_LIST, 
                      submission_limit=SUBMISSION_FETCH_LIMIT, 
                      comment_limit_per_submission=COMMENT_FETCH_LIMIT):
    all_submissions = []
    all_comments = []
    comment_count = 0

    for  subreddit in  subreddit_list:
        try:
            print(f"Processing subreddit: {subreddit}")
            subs, comments = await fetch_submission_and_comments(
                subreddit,
                submission_limit=submission_limit,
                comment_limit_per_submission=comment_limit_per_submission
            )
            
        except Exception as e:
            print(f"[ERROR] Skipping subreddit {subreddit} due to error: {e}")
            continue

        all_submissions.extend(subs)
        all_comments.extend(comments)
                   
                
        await asyncio.sleep(random.uniform(5.0, 8.0))  # avoid rate limits

    return all_submissions, all_comments


# Run and collect both
submissions_result, comments_result = asyncio.run(praw_scrape())

# COMMAND ----------

# DBTITLE 1,Creatting submissiontable
delta_submissions_sql  = f"""
CREATE TABLE IF NOT EXISTS {output_submissions_table} (
    author_name STRING,
    created_utc STRING,
    created_utc_unix BIGINT,
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
    submission_id STRING,
    subreddit STRING,
    title STRING,
    upvote_ratio DOUBLE,
    url STRING,
    ds DATE,
    _loaded_at_timestamp_utc TIMESTAMP
)
USING DELTA
PARTITIONED BY (ds)
TBLPROPERTIES (
    'write.wap.enabled' = 'true'
);
"""
print(f"Executing CREATE TABLE IF NOT EXISTS statement for {output_submissions_table}:\n{delta_submissions_sql}")
try:
    spark.sql(delta_submissions_sql)
    print(f"Table {output_submissions_table} ensured.")
except Exception as e:
    print(f"Error creating table {output_submissions_table}: {e}")


delta_comments_sql = f"""
CREATE TABLE IF NOT EXISTS {output_comments_table} (
    author_name STRING,
    body STRING,
    comment_id STRING,
    created_utc STRING,
    created_utc_unix BIGINT,
    edited STRING,
    is_submitter BOOLEAN,
    parent_id STRING,
    permalink STRING,
    retrieved_utc_unix BIGINT,
    score BIGINT,
    stickied BOOLEAN,
    submission_id STRING,
    ds DATE,
    _loaded_at_timestamp_utc TIMESTAMP
)
USING DELTA
PARTITIONED BY (ds)
TBLPROPERTIES (
    'write.wap.enabled' = 'true'
);
"""

print(f"Executing CREATE TABLE IF NOT EXISTS statement for {output_comments_table}:\n{delta_comments_sql}")
try:
    spark.sql(delta_comments_sql)
    print(f"Table {output_comments_table} ensured.")
except Exception as e:
    print(f"Error creating table {output_comments_table}: {e}")

# COMMAND ----------

# DBTITLE 1,Process subbmissions
# Convert results to a dedup DataFrames
sub_df = (spark.createDataFrame(submissions_result)
          .withColumn('ds', F.lit(ds).cast(T.DateType()))
          .withColumn('_loaded_at_timestamp_utc', F.to_utc_timestamp(F.from_unixtime(F.col("retrieved_utc_unix")), F.lit('UTC'))) # Timestamp when YOUR job loaded it

)

sub_df.printSchema()
sub_df.limit(3).display()


# COMMAND ----------

# DBTITLE 1,Write raw submissions
final_submissions_columns = ['author_name',
                            'created_utc',
                            'created_utc_unix',
                            'edited',
                            'flair',
                            'is_self',
                            'num_comments',
                            'over_18',
                            'permalink',
                            'retrieved_utc_unix',
                            'score',
                            'selftext',
                            'spoiler',
                            'stickied',
                            'submission_id',
                            'subreddit',
                            'title',
                            'upvote_ratio',
                            'url',
                            'ds',
                            '_loaded_at_timestamp_utc']

if sub_df.isEmpty():
    print("No new submissions to write.")
    dbutils.exit("Exiting due to no new submissions.")

submissions_to_write = sub_df.select(*final_submissions_columns)
try:
    (submissions_to_write.writeTo(output_submissions_table)
        .using("delta")
        .partitionedBy("ds")
        .overwritePartitions())
    print(f"Data written successfully to {output_submissions_table} table.")
except Exception as e:
    print(f"Error writing data to table {output_submissions_table}: {e}")


# COMMAND ----------

# DBTITLE 1,process comments
com_df = (spark.createDataFrame(comments_result)
          .withColumn('ds', F.lit(ds).cast(T.DateType()))
          .withColumn('_loaded_at_timestamp_utc', F.to_utc_timestamp(F.from_unixtime(F.col("retrieved_utc_unix")), F.lit('UTC')))
          ).dropDuplicates(['comment_id'])

com_df.printSchema()
com_df.limit(3).display()

# COMMAND ----------

# DBTITLE 1,write raw comments
final_comments_columns = ['author_name',
'body',
'comment_id',
'created_utc',
'created_utc_unix',
'edited',
'is_submitter',
'parent_id',
'permalink',
'retrieved_utc_unix',
'score',
'stickied',
'submission_id',
'ds',
'_loaded_at_timestamp_utc']


if com_df.isEmpty():
    print("No new comments to write.")
    dbutils.exit("Exiting due to no comments.")

comments_to_write = com_df.select(*final_comments_columns)
try:
    (comments_to_write.writeTo(output_comments_table)
        .using("delta")
        .partitionedBy("ds")
        .overwritePartitions())
    print(f"Data written successfully to {output_comments_table} table.")
except Exception as e:
    print(f"Error writing data to table {output_comments_table}: {e}")


# COMMAND ----------


print("thats all folks!!!!!!!!!!")
