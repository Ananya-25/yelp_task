import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from nycs.transformers.data_cleaner import DataCleaner


class TestDataCleaner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder.master("local[2]").appName("unittest").getOrCreate()

    def test_remove_na(self):
        # Create a sample DataFrame
        test_schema = StructType([
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

        # Create a test DataFrame with some null values
        data = [
            ("123", 1, "2022-01-01", 0, "abc123", 4.5, "Good review", 2, "user1"),
            (None, 2, "2022-01-02", 1, "def456", 3.0, "Okay review", 1, "user2"),
            (None, 0, "2022-01-03", 2, "ghi789", 5.0, "Excellent review", 3, None),
            ('345', 0, "2022-01-03", 2, "ghi789", 5.0, "Bad review", 4, None),
            ("123", 1, "2021-01-01", 0, "abc123", None, "Good review", 2, "user1"),
        ]

        test_df = self.spark.createDataFrame(data, schema=test_schema)

        # Define mandatory fields
        mandatory_fields = ["business_id", "user_id"]

        # Apply remove_na method
        cleaned_df = DataCleaner.remove_na(mandatory_fields, test_df)

        # Check that null values are removed
        self.assertEqual(cleaned_df.count(), 4)

    def test_remove_duplicates(self):
        # Create a sample DataFrame
        test_schema = StructType([
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

        data = [
            ("123", 1, "2022-01-01", 0, "abc123", 4.5, "Good review", 2, "user1"),
            ("123", 1, "2022-10-05", 0, "abc123", 4.5, "Okay review", 2, "user1"),
            ("234", 1, "2021-10-01", 0, "dec123", 3.0, "Bad review", 2, "user2"),
        ]
        df = self.spark.createDataFrame(data, schema=test_schema)

        # Define mandatory fields
        mandatory_fields = ["business_id", "user_id"]

        # Apply remove_duplicates method
        cleaned_df = DataCleaner.remove_duplicates(mandatory_fields, df)

        # Check that duplicates are removed
        self.assertEqual(cleaned_df.count(), 2)


if __name__ == '__main__':
    unittest.main()
