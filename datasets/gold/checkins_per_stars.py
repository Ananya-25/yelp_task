from nycs.datasets.base.BaseTableDataset import BaseTableDataset
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


class CheckinsPerStars(BaseTableDataset):
    """
    Represents a dataset storing information about check-ins per stars for businesses.

    Attributes:
    - table_name (str): The name of the table associated with this dataset.
    - schema (StructType): The schema defining the structure of the DataFrame.

    Methods:
    - __init__(self, spark, dir_path: str): Initializes the CheckinsPerStars dataset with Spark session and directory path.
    """
    table_name = 'checkins_per_stars.csv'

    def __init__(self, spark, dir_path: str):
        super().__init__(spark, dir_path)
# Define the schema for the DataFrame
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("checkin_count", LongType(), True),
        StructField("avg_stars", DoubleType(), True)
    ])
