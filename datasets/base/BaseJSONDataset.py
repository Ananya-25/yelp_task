from abc import ABC

from pyspark.sql import DataFrame


class BaseJSONDataset(ABC):
    # Define class variables for mandatory fields, schema, and table name
    mandatory_fields = []  # List of mandatory fields in the table
    schema = []  # Schema definition for the table
    raw_json_filename = ''  # Filename of the raw JSON data

    def __init__(self, spark, dir_path):
        """
        Constructor for BaseJSONDataset.

        Parameters:
        - spark: PySpark session
        - dir_path: Directory path where the JSON data is stored
        """
        self.spark = spark
        self.dir_path = dir_path

    def read(self) -> DataFrame:
        """
        Read the JSON data into a PySpark DataFrame.

        Returns:
        - DataFrame: PySpark DataFrame containing the JSON data
        """
        # Construct the full filepath by combining directory path and raw JSON filename
        filepath = self.dir_path + '/' + self.raw_json_filename
        # Read JSON data into DataFrame with the specified schema
        return self.spark.read.schema(self.schema).json(filepath)
