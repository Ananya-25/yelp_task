from typing import Dict, List

from pyspark.sql import DataFrame

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset


class DataCleaner:
    """
       DataCleaner class for cleaning DataFrames by handling null values and removing duplicates.

       Methods:
       - remove_na(mandatory_fields: List[str], df: DataFrame) -> DataFrame:
         Removes rows with null values in specified mandatory fields from the DataFrame.

       - remove_duplicates(mandatory_fields: List[str], df: DataFrame) -> DataFrame:
         Removes duplicate rows based on specified mandatory fields from the DataFrame.

       Returns:
       - DataFrame: Cleaned DataFrame after handling null values or removing duplicates.
     """
    def __init__(self, spark, datasets):
        """
        Constructor for DataCleaner class.

        Parameters:
        - spark: SparkSession instance.
        - datasets (Dict): Dictionary of datasets.
        """
        self.spark = spark
        self.datasets = datasets

    @staticmethod
    def remove_na(mandatory_fields: List[str], df: DataFrame) -> DataFrame:
        """
        Removes rows with null values in specified mandatory fields from the DataFrame.

        Parameters:
        - mandatory_fields (List[str]): List of mandatory fields for cleaning.
        - df (DataFrame): Input DataFrame to be cleaned.

        Returns:
        - DataFrame: Cleaned DataFrame after handling null values.
        """
        # check na
        expr = [f'{field} IS NOT NULL' for field in mandatory_fields]
        filter_expr = " OR ".join(expr)
        if filter_expr:
            return df.filter(f'{filter_expr}')
        else:
            return df

    @staticmethod
    def remove_duplicates(mandatory_fields: List[str], df: DataFrame):
        """
        Removes duplicate rows based on specified mandatory fields from the DataFrame.

        Parameters:
        - mandatory_fields (List[str]): List of mandatory fields for cleaning.
        - df (DataFrame): Input DataFrame to be cleaned.

        Returns:
        - DataFrame: Cleaned DataFrame after removing duplicates.
        """
        if mandatory_fields:
            return df.drop_duplicates(subset=mandatory_fields)
        else:
            return df


