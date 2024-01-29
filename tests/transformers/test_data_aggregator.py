import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from nycs.transformers.data_aggregator import DataAggregator


class TestDataAggregator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder.master("local[2]").appName("unittest").getOrCreate()

    def test_get_stars_per_business(self):
        # Create sample DataFrames
        business_schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("stars", IntegerType(), True)
        ])
        checkin_schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("week", IntegerType(), True),
            StructField("date", StringType(), True),
        ])
        business_data = [("ABC123", 4), ("DEF456", 3), ("GHI789", 5)]
        checkin_data = [("GHI789", 1, "2022-02-20"), ("DEF456", 2, "2022-01-20"), ("GHI789", 5, "2022-03-20")]
        business_df = self.spark.createDataFrame(business_data, schema=business_schema)
        checkin_df = self.spark.createDataFrame(checkin_data, schema=checkin_schema)

        # Apply get_stars_per_business method
        result_df = DataAggregator.get_stars_per_business(business_df, checkin_df)

        # Check the schema and values
        expected_schema = ["business_id", "week", "avg_stars_per_week"]
        self.assertEqual(expected_schema, result_df.columns)

    def test_get_checkins_per_stars(self):
        # Create sample DataFrames
        business_schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("stars", IntegerType(), True)
        ])
        checkin_schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True)
        ])
        business_data = [("ABC123", 4), ("DEF456", 3), ("GHI789", 5)]
        checkin_data = [("ABC123", "2022-01-15"), ("DEF456", "2022-02-20"), ("ABC123", "2022-01-25")]
        business_df = self.spark.createDataFrame(business_data, schema=business_schema)
        checkin_df = self.spark.createDataFrame(checkin_data, schema=checkin_schema)

        # Apply get_checkins_per_stars method
        result_df = DataAggregator.get_checkins_per_stars(business_df, checkin_df)

        # Check the schema and values
        expected_schema = ["business_id", "checkin_count", "avg_stars"]
        self.assertEqual(expected_schema, result_df.columns)


if __name__ == '__main__':
    unittest.main()
