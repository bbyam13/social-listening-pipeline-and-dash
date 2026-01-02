from pyspark import pipelines as dp
from utilities.utils import build_prompt, build_sentiment_schema, categories
from pyspark.sql.functions import col, expr, lit, concat

@dp.table(
    comment="Extract sentiment and topics from game user generated content"
)
def feedback_content_ai_extraction():
    ai_prompt = build_prompt(categories)
    output_schema = build_sentiment_schema(categories)
    df = spark.readStream.option("skipChangeCommits", "true").table("feedback_content_translated")
    df = df.withColumn(
        "content_topic",
        expr(
            f"ai_query('databricks-meta-llama-3-3-70b-instruct', concat({repr(ai_prompt)}, content_text), '{output_schema}')"
        )
    )
    return df