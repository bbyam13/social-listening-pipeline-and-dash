from ingestion_utils.data_ingestor import DataIngestor
import requests
import math
from datetime import date, datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as PSF
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    LongType,
    MapType,
    TimestampType,
)
import praw

class RedditIngestor(DataIngestor):
    def __init__(self, spark: SparkSession, client_id: str, client_secret: str, user_agent: str):
        """
        Get the client_id, client_secret, user_agent from Reddit developer portal.
        """
        super().__init__(spark)
        self._set_content_type()
        self.full_reddit_schema = StructType(
            [
                StructField("post_id", StringType(), True),
                StructField("comment_id", StringType(), True),
                StructField("body", StringType(), True),
                StructField("author", StringType(), True),
                StructField("score", IntegerType(), True),
                StructField("created_utc", TimestampType(), True),
                StructField("post_title", StringType(), True),
                StructField("post_text", StringType(), True),
            ]
        )
        self.client_id = client_id
        self._client_secret = client_secret
        self._user_agent = user_agent

        # Create Reddit API client
        self._reddit_client = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False
        )

    def ingest(self, subreddit_name: str, max_posts: int=500, max_comments_per_post: int=20,
               start_date: date = None, end_date: date = None) -> DataFrame:
        """
        Get reviews via Reddit API.
        Currently, the output dataframe uses the subreddit name as the "game_name".
        """
        post_rows, comment_rows = self._get_content(subreddit_name=subreddit_name, max_posts=max_posts,
                                                    max_comments_per_post=max_comments_per_post)
        if len(post_rows) == 0:
            # TODO: log warning here
            print("No posts found.")
            return None

        # Define schemas
        post_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("author", StringType(), True),
            StructField("url", StringType(), True),
            StructField("permalink", StringType(), True)
        ])

        comment_schema = StructType([
            StructField("post_id", StringType(), True),
            StructField("comment_id", StringType(), True),
            StructField("body", StringType(), True),
            StructField("author", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("post_title", StringType(), True),
            StructField("post_text", StringType(), True)
        ])

        # Create Spark DataFrames directly
        post_df = self._spark_session.createDataFrame(post_rows, schema=post_schema)
        comment_df = self._spark_session.createDataFrame(comment_rows, schema=comment_schema)

        # Add new columns for static values
        full_df = comment_df.withColumn(
            "subreddit_name", PSF.lit(subreddit_name).cast(StringType())
        )
        full_df = full_df.withColumn(
            "content_type", PSF.lit(self.content_type).cast(StringType())
        )

        # Add unified metadata column
        metadata_cols = [
            "post_id", "score", "post_title", "post_text"
        ]
        full_df = full_df.withColumn(
            "content_metadata", PSF.to_json(PSF.struct(metadata_cols))
        )

        # Map to generic output column schema
        output_df = full_df.select(
            PSF.col("comment_id").alias("content_id"),
            PSF.col("content_type"),
            PSF.col("subreddit_name").alias("game_name"),
            PSF.col("body").alias("content_text"),
            PSF.col("created_utc").alias("timestamp"),
            PSF.col("author").alias("author_id"),
            PSF.col("content_metadata"),
        )
        return output_df

    def _get_content(self, subreddit_name: str, time_filter: str="year", max_posts: int=500,
                     max_comments_per_post: int=20) -> tuple[list]:
        """
        Returns 2 lists: one of post data, one of comment data.
        The comment data list includes associated post information.

        Note: currently always sorts by top for both posts and comments.
        Replies to comments are not extracted currently.
        """
        # Fetch top posts
        top_posts = self._reddit_client.subreddit(subreddit_name).top(time_filter=time_filter, limit=max_posts)
        # print(top_posts)

        # Lists to collect row tuples
        post_rows = []
        comment_rows = []

        for post in top_posts:
            post.comment_sort = "top" # "new" "old" "confidence" (Reddit's "best" filter), "controversial", "q&a"
            post_rows.append((
                post.id,
                post.title,
                post.score,
                datetime.fromtimestamp(post.created_utc, timezone.utc), # datetime.utcfromtimestamp(post.created_utc),
                post.num_comments,
                str(post.author),
                post.url,
                f"https://reddit.com{post.permalink}"
            ))
            # print(post_rows)

            post_title = post.title
            post_text = post.selftext if hasattr(post, "selftext") else ""

            max_comments_per_replacement = 100 # See replace_more() PRAW documentation
            replace_more_limit = math.ceil(max_comments_per_post / max_comments_per_replacement)
            post.comments.replace_more(limit=replace_more_limit)
            comments_flat = post.comments.list() # List of PRAW Comment objects
            new_comment_rows = ( # Not an exhaustive list of fields; note that this must match comment_schema in ingest()
                [(post.id, comment.id, comment.body, str(comment.author), comment.score,
                  datetime.fromtimestamp(comment.created_utc, timezone.utc), post_title, post_text)
                 for comment in comments_flat[:max_comments_per_post]]
            )
            comment_rows += new_comment_rows

            # Original loop implementation
            # for comment in comments_flat[:max_comments_per_post]:
            #     comment_rows.append((
            #         post.id,
            #         comment.id,
            #         comment.body,
            #         str(comment.author),
            #         comment.score,
            #         datetime.utcfromtimestamp(comment.created_utc),
            #         post_title,
            #         post_text
            #     ))
        # print(post_rows)
        return post_rows, comment_rows

    def _get_unique_comment_ids(self, comment_rows: list) -> set:
        """
        Returns a set of unique comment IDs from the output of _get_reviews().
        """
        return set([comment_row[1] for comment_row in comment_rows])

    def _set_content_type(self) -> None:
        self.content_type = "Reddit Comment"