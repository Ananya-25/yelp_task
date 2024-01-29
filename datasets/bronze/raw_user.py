from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset


class RawUser(BaseJSONDataset):
    # Define class variables for mandatory fields and raw JSON filename
    mandatory_fields = ['user_id']  # List of mandatory fields in the "user" table
    # Define class variable for raw JSON filename
    raw_json_filename = "yelp_academic_dataset_user.json"  # Filename of the raw JSON data

    def __init__(self, spark, dir_path):
        """
        Constructor for RawUser.

        Parameters:
        - spark: PySpark session
        - dir_path: Directory path where the JSON data is stored
        """
        super().__init__(spark, dir_path)

    # Define the schema for the "business" table
    schema = StructType([
        StructField("average_stars", DoubleType(), True),
        StructField("compliment_cool", LongType(), True),
        StructField("compliment_cute", LongType(), True),
        StructField("compliment_funny", LongType(), True),
        StructField("compliment_hot", LongType(), True),
        StructField("compliment_list", LongType(), True),
        StructField("compliment_more", LongType(), True),
        StructField("compliment_note", LongType(), True),
        StructField("compliment_photos", LongType(), True),
        StructField("compliment_plain", LongType(), True),
        StructField("compliment_profile", LongType(), True),
        StructField("compliment_writer", LongType(), True),
        StructField("cool", LongType(), True),
        StructField("elite", StringType(), True),
        StructField("fans", LongType(), True),
        StructField("friends", StringType(), True),
        StructField("funny", LongType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", LongType(), True),
        StructField("useful", LongType(), True),
        StructField("user_id", StringType(), True),
        StructField("yelping_since", StringType(), True)
    ])
