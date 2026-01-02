from pyspark import pipelines as dp
from utilities.utils import categories, build_sentiment_schema
from pyspark.sql.functions import (
    col, from_json, explode, map_values, map_keys, 
    named_struct, create_map, lit, when, isnotnull,
    get_json_object
)

@dp.table(
    comment="parse JSON result of sentiment extraction into wide table"
)
@dp.expect_or_drop("valid_overall_sentiment", "overall IS NOT NULL") #drop sentiment with no overall sentiment extracted
def feedback_content_parsed():

  # Generate the schema from our categories
  SENTIMENT_SCHEMA_STRING = build_sentiment_schema(categories)

  # Start with the base columns and parse JSON
  base_columns = [
      col("content_id"),
      col("game_name"), 
      col("author_id"),
      col("content_text"),
      "translated_content_text",
      col("content_type"),
      col("timestamp"),
      col("content_data.overall").alias("overall"),
      col("content_data.summary").alias("summary")
  ]

  # Dynamically add sentiment category columns
  sentiment_columns = []
  for cat in categories:
      sentiment_columns.extend([
          col(f"content_data.{cat}.sentiment").alias(cat),
          col(f"content_data.{cat}.sub_topic").alias(f"{cat}_sub_topic")
      ])

  
  # Parse JSON and extract all fields
  parsed_df = (spark.readStream.option("skipChangeCommits", "true").table("feedback_content_ai_extraction")
    .withColumn("content_data", from_json(col("content_topic"), SENTIMENT_SCHEMA_STRING))
    .select(*base_columns, *sentiment_columns, col("content_data.other").alias("other"), "content_metadata")
  )

  return parsed_df



  