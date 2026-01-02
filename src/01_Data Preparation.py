# Databricks notebook source
# MAGIC %md
# MAGIC # Data Preparation for Game Reviews from Steam

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook connects to the Steam API and returns a DataFrame of reviews for a particular game. The schema returned includes a text review as well as data about the author, the timestamp, and amount of time the author has spent in the game. We will use this data to parse out insights from the data as well as to group results in the dashboard by author groups.
# MAGIC
# MAGIC First we define functions to connect to [the Steam API](https://partner.steamgames.com/doc/webapi_overview#2).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Run Config notebook

# COMMAND ----------

# MAGIC %run "./config/notebook_config"

# COMMAND ----------

dbutils.widgets.dropdown(
    name = "updateType",                   
    defaultValue = "REFRESH",                 
    choices = ["REFRESH", "NEW_GAME"],
    label = "Select Update Type"              
)

dbutils.widgets.text("game_id", '1172470', "id of new game")
dbutils.widgets.text("game_name", "Apex Legends", "name of new game")

# COMMAND ----------

# Check if catalog exists and create if it does not
catalogs = spark.sql("SHOW CATALOGS").collect()
catalog_names = [row.catalog for row in catalogs]

if catalog_name not in catalog_names:
    spark.sql(f"CREATE CATALOG {catalog_name}")

# Set catalog
spark.sql(f"USE CATALOG {catalog_name}")

# Check if database exists and create if it does not
databases = spark.sql("SHOW DATABASES").collect()
database_names = [row.databaseName for row in databases]

if database_name not in database_names:
    spark.sql(f"CREATE DATABASE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define functions to retrieve reviews

# COMMAND ----------

# MAGIC %md
# MAGIC The first step is to create three functions. The first (<i>get_n_reviews</i>) controls the number of reviews and the number per page.

# COMMAND ----------

def get_n_reviews(appid, start_date, end_date, n=100000):
    
    if start_date is None:
        return None ##nothing to process
    
    day_range = (end_date - start_date).days
 
    # call api
    reviews = []
    cursor = '*'
    params = {
            'json' : 1,
            'filter' : 'all',
            'language' : 'all',
            'day_range': day_range,
            'review_type' : 'all',
            'purchase_type' : 'all'
            }

    while n > 0:
        params['cursor'] = cursor.encode()
        params['num_per_page'] = min(100, n)
        n -= 100

        response = get_reviews(appid, params)
        cursor = response['cursor']
        reviews += response['reviews']

        if len(response['reviews']) < 100: break

    return reviews

# COMMAND ----------

# MAGIC %md
# MAGIC The second (<i>get_reviews</i>) sets the URL and makes the request to the API with the parameters that come from <i>get_n_reviews</i>.

# COMMAND ----------

def get_reviews(appid, params={'json':1}):
        url = 'https://store.steampowered.com/appreviews/'
        response = requests.get(url=url+appid, params=params, headers={'User-Agent': 'Mozilla/5.0'})
        response.encoding='utf-8-sig'
        return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC The third (<i>calculate_date_range</i>) determines the date range of reviews to pull from the API based on a table of previous runs.
# MAGIC

# COMMAND ----------

def calculate_date_range(appid):
    
    if spark.catalog.tableExists(f"{database_name}.steam_api_pull_history"):
        run_history_df = spark.read.table(f"{database_name}.steam_api_pull_history")
    else:
        run_history_df = spark.createDataFrame([], run_history_schema)
    
    # Filter for the specific game_id and successful runs
    game_history_df = run_history_df.filter(col("game_id") == appid) 

    # If no runs yet, start from overall_start_date
    if game_history_df.count() == 0:
        start_date = overall_start_date
    else:
        # Get the last run's end_date
        max_end_date = game_history_df.agg(F.max("end_date")).collect()[0][0]
        
        # If max_end_date is None, start from overall_start_date
        if max_end_date is None:
            start_date = overall_start_date
        else:
            # start next run from last run's end_date
            start_date = (max_end_date + timedelta(days=1))
    
    # Set end_date as yesterday
    end_date = (datetime.today() - timedelta(days=1)).date()
    
    # If start_date is after end_date, nothing to fetch
    if start_date > end_date:
        return None, None
    
    return start_date, end_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Use functions and REST API to get reviews

# COMMAND ----------

# MAGIC %md
# MAGIC Use [Steam REST API](https://partner.steamgames.com/doc/webapi_overview#2) to get reviews. 
# MAGIC <br>
# MAGIC <br>
# MAGIC <b>Note:</b> You can get an app ID by looking up a game on [the Steam website](https://store.steampowered.com/) and getting the ID from the URL. In the <i>review_id</i> dictionary below we include a few IDs.

# COMMAND ----------

updateType = dbutils.widgets.get("updateType")

#pull new game from parameters if adding new game
if updateType == "NEW_GAME":
  game_id = dbutils.widgets.get("game_id")
  game_name = dbutils.widgets.get("game_name")
  review_id = {game_name: game_id}

#check run history table for games to refresh
elif spark.catalog.tableExists(f"{database_name}.steam_api_pull_history"):
  games_to_refresh_df = spark.read.table(f"{database_name}.steam_api_pull_history").select("game_name", "game_id").distinct()
  games_to_refresh = {row["game_name"]: row["game_id"] for row in games_to_refresh_df.collect()}
  review_id = games_to_refresh

# use default games if first run
else:
  review_id = {"Apex Legends":'1172470',
             "Call of Duty: Black Ops 6": '2933620',
             "New World":'1063730',
             "Elden Ring":'1245620',
             "Mafia 3":'360430',
             "GTA":'1546990',
             "God of War":'2322010',
             "Payday 3":'1272080',
             "Wukong":'2358720'}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and subset data

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have reviews, let's reduce down to an actionable subset and create a Spark DataFrame.

# COMMAND ----------


custom_schema = StructType([
    StructField("author", MapType(StringType(), StringType()), True),
    StructField("comment_count", LongType(), True),
    StructField("language", StringType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("recommendationid", StringType(), True),
    StructField("review", StringType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("timestamp_created", LongType(), True),
    StructField("timestamp_updated", LongType(), True),
    StructField("voted_up", BooleanType(), True),
    StructField("votes_funny", LongType(), True),
    StructField("votes_up", LongType(), True),
    StructField("weighted_vote_score", StringType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("game_name", StringType(), True)
])

custom_schema_intermediate = StructType([
    StructField("author", MapType(StringType(), StringType()), True),
    StructField("comment_count", LongType(), True),
    StructField("language", StringType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("recommendationid", StringType(), True),
    StructField("review", StringType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("timestamp_created", LongType(), True),
    StructField("timestamp_updated", LongType(), True),
    StructField("voted_up", BooleanType(), True),
    StructField("votes_funny", LongType(), True),
    StructField("votes_up", LongType(), True),
    StructField("weighted_vote_score", StringType(), True),
    StructField("written_during_early_access", BooleanType(), True)
])

run_history_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("execution_date", DateType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_name", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True)
])


# Initialize an empty DataFrame
all_reviews_df = spark.createDataFrame([], custom_schema)
run_history_update_df = spark.createDataFrame([], run_history_schema)
execution_date = datetime.today()
run_id = uuid.uuid4()
    
for game_name, appid in review_id.items():

    start_date, end_date = calculate_date_range(appid)
    reviews = get_n_reviews(appid, start_date, end_date)
    
    if reviews is not None:
        # Create DataFrame for the current game's reviews
        reviews_df = spark.createDataFrame(data=reviews, schema=custom_schema_intermediate)

        # Add a new column with the game name as a static value
        reviews_df = reviews_df.withColumn("game_name", lit(game_name).cast(StringType()))

        # update run history
        run_history_data = [(run_id, execution_date,appid,game_name, start_date, end_date)]
        run_history_update_row = spark.createDataFrame(run_history_data, run_history_schema)
        run_history_update_df = run_history_update_df.union(run_history_update_row)
        
        # Union the current game's reviews with the all_reviews_df
        all_reviews_df = all_reviews_df.union(reviews_df)

display(run_history_update_df)

# COMMAND ----------

display(all_reviews_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, filter down to where the review is not null.

# COMMAND ----------

reviewsDF_filtered = all_reviews_df \
                     .filter((all_reviews_df.review.isNotNull())) \
                     #.select('review','author', 'game_name')
#display(reviewsDF_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC Explode out author columns. This data will be valuable in segmenting reviews based on types of users to target different audiences.

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

reviewsDF_filtered = reviewsDF_filtered \
        .select(col("recommendationid"),
                col("game_name"),
                col("review"),
                col("comment_count").cast("int"),
                col("received_for_free").cast("boolean"),
                col("steam_purchase").cast("boolean"),
                from_unixtime(col("timestamp_created")).cast("timestamp").alias("timestamp_created"),
                from_unixtime(col("timestamp_updated")).cast("timestamp").alias("timestamp_updated"),
                col("voted_up").cast("boolean"),
                col("votes_funny").cast("int"),
                col("votes_up").cast("int"),
                col("weighted_vote_score").cast("float"),
                col("written_during_early_access").cast("boolean"),
                col("author.steamid").alias("author_steamid"),
                col("author.num_reviews").alias("author_num_reviews").cast("int"),
                col("author.num_games_owned").alias("author_num_games_owned").cast("int"),
                from_unixtime(col("author.last_played")).alias("author_last_played").cast("timestamp"),
                col("author.playtime_forever").alias("author_playtime_forever").cast("int"),
                col("author.playtime_at_review").alias("author_playtime_at_review").cast("int"),
                col("author.playtime_last_two_weeks").alias("author_playtime_at_last_two_weeks").cast("int")
               )

# COMMAND ----------

display(reviewsDF_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC The <i>01_Data Preparation</i> notebook outputs:
# MAGIC * A DataFrame called *reviewsDF_filtered* which has: *game_name*, *review*, and *author_cols*
# MAGIC * A Python list called *author_cols* that lists the columns related to authors

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Output reviewsDF_filtered to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC Add data from the last point of the id/reviews that were read to avoid rate limiting
# MAGIC
# MAGIC Filter out duplicate Recommendations: Remove reviews already existing in output table based on _recommendationid_

# COMMAND ----------

# Check if table exists and create if it does not
table_name = "steam_reviews_bronze"

tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()
table_names = [row.tableName for row in tables]

if table_name not in table_names:
    # Create the table if it does not exist
    reviewsDF_filtered.write \
        .format("delta") \
        .saveAsTable(f"{database_name}.{table_name}")
else:
    
    # Load the existing table
    existing_reviews_df = spark.table(f"{database_name}.{table_name}").select("recommendationid")

    # Filter records that don't exist in the existing table
    new_reviews_df = reviewsDF_filtered.join(existing_reviews_df, on="recommendationid", how="left_anti")
    
    # Append data to the existing table
    reviewsDF_filtered.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC update run history

# COMMAND ----------

run_history_update_df.write \
  .mode("append") \
  .saveAsTable(f"{database_name}.steam_api_pull_history")
