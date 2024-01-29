from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from nycs.datasets.base.BaseTableDataset import BaseTableDataset


class CleanCheckin(BaseTableDataset):
    """
    Represents a clean dataset storing information about check-ins for businesses.

    Attributes:
    - table_name (str): The name of the table associated with this dataset.
    - schema (StructType): The schema defining the structure of the DataFrame.

    Methods:
    - __init__(self, spark, dir_path: str): Initializes the CleanCheckin dataset with Spark session and directory path.
    """
    table_name = "checkins"

    def __init__(self, spark, dir_path: str):
        super().__init__(spark, dir_path)

    # Define the schema
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("week", IntegerType(), True)
    ])
