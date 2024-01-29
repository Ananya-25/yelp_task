from pyspark.sql import DataFrame
from pyspark.sql.functions import weekofyear


class DataAugmentor:
    """
       DataAugmentor class for augmenting DataFrame with week information.

       Methods:
       - add_week_of_year(df: DataFrame, date_col_name: str) -> DataFrame:
         Adds a new column 'week' to the DataFrame, extracting the week number from the specified date column.

     """

    @staticmethod
    def add_week_of_year(df: DataFrame, date_col_name: str):
        """
        Adds a new column 'week' to the DataFrame, extracting the week number from the specified date column.

        Parameters:
        - df (DataFrame): Input DataFrame to be augmented.
        - date_col_name (str): Name of the date column from which to extract the week number.

        Returns:
        - DataFrame: Augmented DataFrame with an additional 'week' column.
        """
        return df.withColumn("week", weekofyear(date_col_name))
