# Databricks notebook source
# MAGIC %md
# MAGIC ## Install and import necessary packages

# COMMAND ----------

# MAGIC %pip install demoji spark-nlp==4.2.0 git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

import os
import requests

#Unicodedata module provides access to the Unicode Character Database (UCD)
import unicodedata as uni

#Demoji helps to process emojis
import demoji

#re for regular expressions
import re

from pyspark.sql.functions import lit

from pyspark.ml import Pipeline
import pyspark.sql.functions as F
from pyspark.sql.functions import col,udf
from pyspark.sql.types import StringType

from pyspark.sql.types import *
from datetime import date,timedelta,datetime
from pyspark.sql import functions as F
import uuid


# COMMAND ----------

# MAGIC %md
# MAGIC ## Set variables and paths

# COMMAND ----------

useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = useremail.split('@')[0]
username_sql = re.sub('\W', '_', username)
tmpdir = f"/dbfs/tmp/{username}/"
tmpdir_dbfs = f"/tmp/{username}"
catalog_name = dbutils.widgets.get("catalog")
database_name = dbutils.widgets.get("schema")
#set start date of when to start pulling reviews from
overall_start_date = date(2025, 1, 1)

# COMMAND ----------

os.environ['tmpdir'] = tmpdir

# COMMAND ----------

# MAGIC %md
# MAGIC If you wish to use a different game, you can specify it in the notebook_config, by looking up your game on the Steam website and getting the ID from the URL. If no game is specified, we will default to New World.
# MAGIC
# MAGIC If you use one of the games in the dictionary in the 01 notebook, specifying the <i>game_name</i> is sufficient. Otherwise, please specify both <i>user_game_id</i> and <i>game_name</i>. <i>game_name</i> is used downstream to name the output table.

# COMMAND ----------

# MAGIC %md
# MAGIC <i>author_cols</i> specifies the columns from steam that have metadata about the reviewer/author, we use this list throughout the notbooks.

# COMMAND ----------

author_cols = ["author_steamid", "author_num_reviews", "author_num_games_owned", \
               "author_last_played", "author_playtime_forever", \
               "author_playtime_at_review", "author_playtime_at_last_two_weeks" \
              ]

# COMMAND ----------

llm_endpoint_name = 'databricks-meta-llama-3-3-70b-instruct'

llm_parameters = {
    "max_tokens": 1000,
    "temperature": 0.20
}

persona_prompts = {
  'community_manager':  'You are an expert community manager assistant. Based only on the player generated content items provided, write a concise and human-readable report.\n\nStart with a section titled "Summary" at the top. In the summary, include the overall sentiment, main drivers of sentiment, and any emerging trends or notable data points, even if they are not covered by the questions below. End the summary with a single sentence using exact, direct language from up to three content items if it adds clarity or impact. Do not quote content elsewhere in the report.\n\nAfter the summary, address these points and group similar insights under a single theme oranganized by markdown titles:: What proportion of content items mention community toxicity or positive community interactions? What topics are often mentioned as suggestions for improvement? What topics are highly praised by the community? Which pain points are most associated with churn? Are there any gameplay mechanics being exploited or causing the game to be unbalanced or unfair?\nUse narrative paragraphs, not bullet points. Group similar feedback under clear labels and avoid redundancy. Keep the report short, focused, and easy to read.\n\nContext: {context}',

  'marketer': 'Write a concise, human-readable summary for a marketer based on the player generated content items provided.\nAlways begin the response with a "Summary" section at the top.\nThis section should include overall sentiment, main drivers, and any emerging trends or notable data points-even if they are not covered by the specific questions below.\nUse exact, direct language from up to three content items if it adds clarity or impact in one sentence only at the end of the summary.\nDo not quote content again throughout the response.\nAfter the summary, address the following specific data points, \n Group similar insights from multiple content items under a single theme oranganized by markdown titles:\nWhat keywords in positive content items correlate most strongly with high sentiment scores?\nWhich game features are most often praised in content items with high playtime?\nWhich pain points are most associated with churn?\nWhich game features are most associated with replayability and retention?\nUse narrative paragraphs and separate themes with paragraph titles.\nAvoid redundant or overlapping points; group similar feedback under a single, clear label.\nKeep the report short, focused, and easy to read.\n\nContent: {context}',

  'game_designer': 'Write a concise, human-readable summary for a game designer on the player generated content items provided. Always begin the response with a "Summary" section at the top.\nThis section should include overall sentiment, main drivers, and any emerging trends or notable data points-even if they are not covered by the specific questions below.\nUse exact, direct language from up to three content items if it adds clarity or impact in one sentence only at the end of the summary.\nDo not quote content again throughout the response.\nAfter the summary, address the following specific data points and group them under a single theme oranganized by markdown titles:\nAre any gameplay mechanics or onboarding steps frequently mentioned as confusing or frustrating?\nWhat are the top three gameplay mechanics cited as pain points in negative content items?\nWhat are the top three gameplay mechanics cited as plus points in positive content items?\nAre there any gameplay mechanics being exploited or causing the game to be unbalanced or unfair?\nAre there general gameplay balance concerns (characters, weapons, etc)?\nUse narrative paragraphs and only use bullets if they aid clarity.\nAvoid redundant or overlapping points; group similar feedback under a single, clear label.\nKeep the report short, focused, and easy to read.\n\nContent: {context}'
}

