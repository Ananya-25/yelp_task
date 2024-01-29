from pyspark.sql.types import StructType, StructField, StringType, LongType

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset


class RawTip(BaseJSONDataset):
    # Define class variables for mandatory fields and raw JSON filename
    mandatory_fields = ['business_id', 'user_id']  # List of mandatory fields in the "tip" table
    raw_json_filename = "yelp_academic_dataset_tip.json"  # Filename of the raw JSON data

    def __init__(self, spark, dir_path):
        """
        Constructor for RawTip.

        Parameters:
        - spark: PySpark session
        - dir_path: Directory path where the JSON data is stored
        """
        super().__init__(spark, dir_path)

    # Define the schema for the "business" table
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("compliment_count", LongType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("user_id", StringType(), True)
    ])
