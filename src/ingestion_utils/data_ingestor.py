from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import random

class DataIngestor(ABC):
    """Virtual class to be inherited by source-specific Ingestor classes."""

    def __init__(self, spark: SparkSession):
        self.output_schema = {
            "content_id": "string",
            "content_type": "string",
            "game_name": "string",
            "content_text": "string",
            "timestamp": "timestamp",
            "author_id": "string",
            "content_metadata": "string"}
        self._spark_session = spark
        
    @abstractmethod
    def ingest(self) -> DataFrame:
        pass

    @abstractmethod
    def _set_content_type(self) -> None:
        self.content_type = "None"
    
    @staticmethod
    def _sample_content(content_list: list, pre_sampling_content_threshold: int=2000) -> list:
        """
        Sample uniformly from a list of content (e.g. Steam reviews).
        The "pre_sampling_content_threshold" arg. is the minimum number of content items before
        sampling occurs (e.g. if the given list has less than this number of items, no sampling/filtering occurs).
        """
        if len(content_list) <= pre_sampling_content_threshold:
            return content_list
        return random.sample(content_list, pre_sampling_content_threshold)