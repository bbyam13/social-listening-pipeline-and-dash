from pyspark import pipelines as dp
from pyspark.sql.functions import col,lit, explode, named_struct, create_map
from utilities.utils import categories

# Please edit the sample below
@dp.table
def feedback_content_gold():
    df = spark.readStream.option("skipChangeCommits", "true").table("feedback_content_parsed").select(
        "content_id",
        "game_name",
        "content_text",
        "translated_content_text",
        "content_type",
        "author_id",
        "timestamp",
        col("overall").alias("overall_sentiment"),
        col("summary").alias("ai_summary"),
        "content_metadata",
    )

    return df

@dp.table
def feedback_content_sentiment_gold():

    parsed_df = spark.readStream.option("skipChangeCommits", "true").table("feedback_content_parsed")
    
    # Build a map to pivot the data;  "gameplay_mechanics" -> {sentiment: "positive", sub_topic: "controls"}
    map_elements = []
    for cat in categories:
        map_elements.extend([
            lit(cat),
            named_struct(
                lit("sentiment"), col(cat),
                lit("sub_topic"), col(f"{cat}_sub_topic")
            )
        ])

    # process specified sentiment categories - # explode map into separate rows
    cat_sentiments = (
        parsed_df
        .select(
            "content_id",
            "game_name",
            explode(create_map(*map_elements)).alias("category", "sentiment_info")
        )
        .select(
            "content_id",
            "game_name",
            "category",
            col("sentiment_info.sentiment").alias("sentiment"),
            col("sentiment_info.sub_topic").alias("sub_category")
        )
        .filter(col("sentiment").isNotNull())
    )

    # process other topics extracted from LLM - # explode col into separate rows
    other_sentiments = (
        parsed_df
        .select(
            "content_id",
            "game_name",
            explode(col("other")).alias("other_topic")
        )
        .select(
            "content_id",
            "game_name",
            lit("other").alias("category"),
            col("other_topic.sentiment").alias("sentiment"),
            col("other_topic.topic").alias("sub_category")
        )
        .filter(col("sentiment").isNotNull())
    )

    # union other topics and specified sentiment categories
    return cat_sentiments.union(other_sentiments)