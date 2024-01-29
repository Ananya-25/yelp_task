from pyspark.sql.types import StructType, StructField, StringType, LongType

from nycs.datasets.base.BaseTableDataset import BaseTableDataset


class CleanTip(BaseTableDataset):
    """
    Represents a clean dataset storing information about tips related to businesses.

    Attributes:
    - table_name (str): The name of the table associated with this dataset.
    - schema (StructType): The schema defining the structure of the DataFrame.

    Methods:
    - __init__(self, spark, dir_path: str): Initializes the CleanTip dataset with Spark session and directory path.
    """
    table_name = "tips"

    def __init__(self, spark, dir_path):
        super().__init__(spark, dir_path)

    # Define the schema
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("compliment_count", LongType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("user_id", StringType(), True)
    ])
