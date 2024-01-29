import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col

from nycs.transformers.data_augmentor import DataAugmentor


class TestDataAugmentor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder.master("local[2]").appName("unittest").getOrCreate()

    def test_add_week_of_year(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True)
        ])
        data = [("GHI789", "2022-01-15"), ("DEF456", "2022-02-20"), ("GHI789", "2022-03-25")]
        df = self.spark.createDataFrame(data, schema=schema)

        # Convert the "date" column to DateType
        df = df.withColumn("date", col("date").cast(DateType()))

        # Apply add_week_of_year method
        augmented_df = DataAugmentor.add_week_of_year(df, "date")

        # Check that the "week" column is added
        self.assertTrue("week" in augmented_df.columns)

        # Check the values of the "week" column
        expected_weeks = [2, 7, 12]
        actual_weeks = [row["week"] for row in augmented_df.collect()]
        self.assertEqual(expected_weeks, actual_weeks)


if __name__ == '__main__':
    unittest.main()