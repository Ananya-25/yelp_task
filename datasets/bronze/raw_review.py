from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


class RawReview(BaseJSONDataset):
    # Define class variables for mandatory fields and raw JSON filename
    mandatory_fields = ['business_id']  # List of mandatory fields in the "review" table
    # Define class variable for raw JSON filename
    raw_json_filename = "yelp_academic_dataset_review.json"  # Filename of the raw JSON data

    def __init__(self, spark, dir_path):
        """
        Constructor for RawReview.

        Parameters:
        - spark: PySpark session
        - dir_path: Directory path where the JSON data is stored
        """
        super().__init__(spark, dir_path)

    # Define the schema for the "business" table
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("cool", LongType(), True),
        StructField("date", StringType(), True),
        StructField("funny", LongType(), True),
        StructField("review_id", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("text", StringType(), True),
        StructField("useful", LongType(), True),
        StructField("user_id", StringType(), True)
    ])
