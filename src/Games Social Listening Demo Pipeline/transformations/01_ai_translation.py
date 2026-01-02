from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.table(
    comment="Translate Content to English"
)
def feedback_content_translated():
    df = spark.readStream.option("skipChangeCommits", "true").table("feedback_content_bronze")
    df = df.withColumn(
        "translated_content_text",
        expr("ai_translate(content_text, 'en')")
    )
    return df