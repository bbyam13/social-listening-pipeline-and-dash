from ingestion_utils.data_ingestor import DataIngestor
import requests
from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as PSF
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, MapType, TimestampType

class SteamIngestor(DataIngestor):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._set_content_type()
        self.full_steam_schema = StructType([
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
            StructField("primarily_steam_deck", BooleanType(), True)
        ])

    def ingest(self, app_id: str, game_name: str, num_reviews: int=10_000, sample: bool=True, start_date: date=None, end_date: date=None) -> DataFrame:
        """Get reviews via Steam API. App ID can found from the corresponding Steam store page URL."""
        reviews = self._get_n_reviews(app_id, num_reviews, start_date, end_date)
    
        if len(reviews) == 0:
            # TODO: log warning here
            return None
        
        # Sample reviews (to reduce data/compute for speed/demo purposes)
        if sample:
            reviews = self._sample_content(reviews)

        reviews_df = self._spark_session.createDataFrame(data=reviews, schema=self.full_steam_schema)

        # Add a new columns
        reviews_df = reviews_df.withColumn("game_name", PSF.lit(game_name).cast(StringType()))
        reviews_df = reviews_df.withColumn("content_type", PSF.lit(self.content_type).cast(StringType()))
        reviews_df = reviews_df.withColumn("steamid", PSF.col("author").getItem("steamid").cast(StringType()))
        reviews_df = reviews_df.withColumn("timestamp_created_formatted", PSF.from_unixtime("timestamp_created").cast(TimestampType()))

        # Add unified metadata column
        metadata_cols = [
            "author", "comment_count", "language", "received_for_free", "steam_purchase", "timestamp_created", "timestamp_updated",
            "voted_up", "votes_funny", "votes_up", "weighted_vote_score", "written_during_early_access", "primarily_steam_deck"
        ]
        reviews_df = reviews_df.withColumn(
            "content_metadata",
            PSF.to_json(PSF.struct(metadata_cols))
        )

        output_df = reviews_df.select(
            PSF.col("recommendationid").alias("content_id"),
            PSF.col("content_type"),
            PSF.col("game_name"),
            PSF.col("review").alias("content_text"),
            PSF.col("timestamp_created_formatted").alias("timestamp"),
            PSF.col("steamid").alias("author_id"),
            PSF.col("content_metadata")
        )
        return output_df

    def _get_n_reviews(self, app_id: str, num_reviews: int=10_000, start_date: date=None, end_date: date=None) -> list:
        """
        Retrieve the specified number of reviews from Steam.
        Both start_date and end_date must provided to get reviews in a certain date range.
        Currently the reviews are sorted by most recent update time.
        """
        params = {
            'json' : 1,
            'filter' : 'recent', # all, recent, updated (with pagination, "recent"/"updated" should be used (see Steam API docs))
            'language' : 'all',
            'review_type' : 'all',
            'purchase_type' : 'all'
        }
        if start_date and end_date:
            day_range = (end_date - start_date).days
            params['day_range'] = day_range

        reviews = []
        cursor = '*' # For pagination
        max_reviews_per_page = 100
        reviews_fetched = 0
        while reviews_fetched < num_reviews:
            params['cursor'] = cursor.encode()
            params['num_per_page'] = min(max_reviews_per_page, num_reviews - reviews_fetched)

            response = self._get_reviews(app_id, params)
            reviews += response['reviews']
            reviews_fetched += len(response['reviews'])

            if (len(response['reviews']) == 0): # < params['num_per_page']):
                print("No more reviews available")
                break
            cursor = response['cursor']
        return reviews
    
    def _get_reviews(self, app_id: str, params={'json':1}):
        reviews_url = "https://store.steampowered.com/appreviews/" + app_id
        response = requests.get(
            url=reviews_url,
            params=params,
            headers={'User-Agent': 'Mozilla/5.0'})
        response.encoding='utf-8-sig'
        raw_dict = response.json()
        return raw_dict
    
    def _get_unique_review_ids(self, reviews: list) -> set:
        """Converts the output from _get_n_reviews() into a set of unique review IDs."""
        return set([review['recommendationid'] for review in reviews])

    def _set_content_type(self) -> None:
        self.content_type = "Steam Review"