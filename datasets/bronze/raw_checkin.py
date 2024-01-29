from pyspark.sql.types import StructType, StructField, StringType

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset


class RawCheckin(BaseJSONDataset):
    # Define class variables for mandatory fields and raw JSON filename
    mandatory_fields = ['business_id', 'date']  # List of mandatory fields in the "checkin" table
    # Define class variable for raw JSON filename
    raw_json_filename = "yelp_academic_dataset_checkin.json"  # Filename of the raw JSON data

    def __init__(self, spark, dir_path):
        """
        Constructor for RawCheckin.

        Parameters:
        - spark: PySpark session
        - dir_path: Directory path where the JSON data is stored
        """
        super().__init__(spark, dir_path)

    # Define the schema for the "business" table
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("date", StringType(), True)
    ])
