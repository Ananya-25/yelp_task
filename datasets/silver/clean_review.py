from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from nycs.datasets.base.BaseTableDataset import BaseTableDataset


class CleanReview(BaseTableDataset):
    """
    Represents a clean dataset storing information about reviews for businesses.

    Attributes:
    - table_name (str): The name of the table associated with this dataset.
    - schema (StructType): The schema defining the structure of the DataFrame.

    Methods:
    - __init__(self, spark, dir_path: str): Initializes the CleanReview dataset with Spark session and directory path.
    """
    table_name = "reviews"

    def __init__(self, spark, dir_path: str):
        super().__init__(spark, dir_path)

    # Define the schema
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
