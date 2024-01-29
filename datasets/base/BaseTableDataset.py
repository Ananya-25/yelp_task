from abc import ABC

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class BaseTableDataset(ABC):
    # Define class variables for mandatory fields, schema, and table name
    mandatory_fields: List[str] = []  # List of mandatory fields in the table
    schema: StructType = None  # Schema definition for the table
    table_name: str = ''  # Name of the table in the Spark catalog
    df: DataFrame = None

    def __init__(self, spark, dir_path: str):
        """
        Constructor for BaseTableDataset.

        Parameters:
        - spark: PySpark session
        - dir_path: str pointing to path where to store data
        """
        self.spark = spark
        self.dir_path = dir_path

    def read(self) -> DataFrame:
        """
        Read the data from the table into a PySpark DataFrame.

        Returns:
        - DataFrame: PySpark DataFrame containing the table data
        """
        return self.spark.read.schema(self.schema).table(self.table_name)

    def write(self, df: DataFrame) -> None:
        """
        Write the DataFrame to a Delta table, overwriting existing data.

        Parameters:
        - df (DataFrame): The DataFrame to be written.

        Returns:
        - None
        """
        df.write.format('parquet').mode('overwrite').save(self.table_name)

    def save(self) -> None:
        """
        Save the DataFrame to a Parquet file, overwriting existing data.

        The location to save the Parquet file is determined by the combination of the specified
        directory path and table name.

        Parameters:
        - None

        Returns:
        - None
        """
        self.df.write.format('parquet').mode('overwrite').save(self.dir_path + self.table_name)

    def save_as_csv(self) -> None:
        """
        Save the DataFrame to a CSV file, overwriting existing data.

        The location to save the CSV file is determined by the combination of the specified
        directory path and table name.

        Parameters:
        - None

        Returns:
        - None
        """
        self.df.write.format('csv').mode('overwrite').save(self.dir_path + self.table_name)
