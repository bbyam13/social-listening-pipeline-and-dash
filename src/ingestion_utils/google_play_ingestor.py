from ingestion_utils.data_ingestor import DataIngestor
import google_play_scraper as GPS
from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as PSF
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

class GooglePlayIngestor(DataIngestor):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._set_content_type()
        self.full_review_schema = StructType([
            StructField("reviewId", StringType(), True),
            StructField("userName", StringType(), True),
            StructField("userImage", StringType(), True),
            StructField("content", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("thumbsUpCount", LongType(), True),
            StructField("reviewCreatedVersion", StringType(), True),
            StructField("at", TimestampType(), True),
            StructField("replyContent", StringType(), True),
            StructField("repliedAt", TimestampType(), True),
            StructField("appVersion", StringType(), True)
        ])

        # As of Nov 2025, the google_play_scraper package does not support scraping
        # from all countries/languages at once.
        # For now, these lists will be used (looped) to get reviews for multiple countries/languages.
        # See the google_play_scraper package repo for more info.
        # Also, it seems that the "country" flag in the Google Play API makes no difference between
        # any supported country code, so "us" will be used for all languages.
        self.supported_languages = ["en", "fr", "es", "ja", "ko", "zh", "pt", "hi"]

    def ingest(self, app_id: str, game_name: str, num_reviews: int=10_000, sample: bool=True, 
               start_date: date=None, end_date: date=None) -> DataFrame:
        """
        Get the specified number of reviews from the Google Play Store.
        The app_id should be in the form: "com.nianticlabs.pokemongo"
        (You can find this from the end of the store page URL: id={app_id})

        Note that the number of "reviews" reported on the store page is actually the number
        of ratings (e.g. 1-5 star). This function only gets actual reviews (ratings with written comments)
        """
        reviews = self._get_reviews(app_id, num_reviews)
    
        if len(reviews) == 0:
            # TODO: log warning here
            return None
        
        # Sample reviews (to reduce data/compute for speed/demo purposes)
        if sample:
            reviews = self._sample_content(reviews)

        
        reviews_df = self._spark_session.createDataFrame(data=reviews, schema=self.full_review_schema)

        # Add a new columns
        reviews_df = reviews_df.withColumn("game_name", PSF.lit(game_name).cast(StringType()))
        reviews_df = reviews_df.withColumn("content_type", PSF.lit(self.content_type).cast(StringType()))

        # Add unified metadata column
        metadata_cols = ["userImage", "score", "thumbsUpCount", "reviewCreatedVersion",
                         "replyContent", "repliedAt", "appVersion"]
        reviews_df = reviews_df.withColumn(
            "content_metadata",
            PSF.to_json(PSF.struct(metadata_cols))
        )

        output_df = reviews_df.select(
            PSF.col("reviewId").alias("content_id"),
            PSF.col("content_type"),
            PSF.col("game_name"),
            PSF.col("content").alias("content_text"),
            PSF.col("at").alias("timestamp"),
            PSF.col("userName").alias("author_id"),
            PSF.col("content_metadata")
        )
        return output_df
    
    def _get_reviews(self, app_id: str, num_reviews: int=10_000) -> list:
        """
        Get the number of desired reviews, sorting by most recent.
        """
        # Language-handling strategy:
        # 1. Attempt to get num_reviews from each language
        # 2. Sample num_reviews from resulting list uniformly (to maintain language ratio)
        all_reviews_all_languages = []
        for language in self.supported_languages:
            print("Getting reviews for language: " + language)
            all_reviews = []
            continuation_token = None
            reviews_per_page = 100
            reviews_fetched = 0
            while reviews_fetched < num_reviews:
                # Note: the response from GPS.reviews() does not seem to honor the "count" param,
                # and instead always gets reviews_per_page (potentially leading to overshoot).
                # This is no issue as sampling afterward gets the exact number.
                reviews, continuation_token = GPS.reviews(
                    app_id,
                    lang=language, # defaults to 'en'
                    country='us', # defaults to 'us'
                    sort=GPS.Sort.NEWEST, # defaults to Sort.NEWEST; other options: MOST_RELEVANT, RATING
                    count=min(reviews_per_page, num_reviews - reviews_fetched), # defaults to 100, max 200
                    continuation_token=continuation_token
                )
                # print("\tcount: " + str(min(reviews_per_page, num_reviews - reviews_fetched)))

                all_reviews += reviews
                reviews_fetched += len(reviews)

                # print("\tnew reviews: " + str(len(reviews)))
                # print("\tlen(all_reviews): " + str(len(all_reviews)))
                # print("\treviews fetched: " + str(reviews_fetched))
                if len(reviews) < reviews_per_page:
                    print("No more reviews available for language: " + language)
                    break
            all_reviews_all_languages += all_reviews
        sampled_reviews = self._sample_content(all_reviews_all_languages, num_reviews)
        return sampled_reviews

    def _get_unique_review_ids(self, all_reviews: list) -> set:
        """
        Returns a set of unique review IDs from the output of _get_reviews().
        """
        return set([review["reviewId"] for review in all_reviews])

    def _set_content_type(self) -> None:
        self.content_type = "Google Play Review"