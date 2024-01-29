from nycs.datasets.base.BaseTableDataset import BaseTableDataset
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


class StarsPerBusiness(BaseTableDataset):
    """
    Represents a dataset storing information about average stars per week for businesses.

    Attributes:
    - table_name (str): The name of the table associated with this dataset.
    - schema (StructType): The schema defining the structure of the DataFrame.

    Methods:
    - __init__(self, spark, dir_path: str): Initializes the StarsPerBusiness dataset with Spark session and directory path.
    """
    table_name = 'stars_per_business.csv'

    def __init__(self, spark, dir_path: str):
        super().__init__(spark, dir_path)

    # Define the schema for the "stars_per_business" table
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("week", IntegerType(), True),
        StructField("avg_stars_per_week", DoubleType(), True)
    ])
