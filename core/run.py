from typing import Dict

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset

from pyspark.sql import SparkSession, DataFrame

from nycs.datasets.base.BaseTableDataset import BaseTableDataset
from nycs.datasets.bronze.raw_business import RawBusiness
from nycs.datasets.bronze.raw_checkin import RawCheckin
from nycs.datasets.bronze.raw_review import RawReview
from nycs.datasets.bronze.raw_tip import RawTip
from nycs.datasets.bronze.raw_user import RawUser
from nycs.datasets.gold.checkins_per_stars import CheckinsPerStars
from nycs.datasets.gold.stars_per_business import StarsPerBusiness
from nycs.datasets.silver.clean_business import CleanBusiness
from nycs.datasets.silver.clean_checkin import CleanCheckin
from nycs.datasets.silver.clean_review import CleanReview
from nycs.datasets.silver.clean_tip import CleanTip
from nycs.datasets.silver.clean_user import CleanUser
from nycs.tasks.aggregator import Aggregator
from nycs.tasks.preprocessor import Preprocessor

BRONZE_DATA_DIR_PATH = 'data/bronze/'
SILVER_DATA_DIR_PATH = 'data/silver/'
GOLD_DATA_DIR_PATH = 'data/gold/'


class Application:
    def __init__(self) -> None:
        # Initialize Spark session and datasets
        self.spark = self.init_spark()
        self.raw_datasets = self.init_raw_datasets(BRONZE_DATA_DIR_PATH)
        self.clean_datasets = self.init_clean_datasets(SILVER_DATA_DIR_PATH)
        self.aggregated_datasets = self.init_aggregated_datasets(GOLD_DATA_DIR_PATH)
        # Perform preprocessing and aggregation on the datasets
        Preprocessor(self.raw_datasets, self.clean_datasets).transform()
        Aggregator(self.clean_datasets, self.aggregated_datasets).transform()

    @staticmethod
    def init_spark() -> SparkSession:
        """Start spark session."""
        session = SparkSession.builder.appName("YelpDataLake").getOrCreate()
        return session

    def init_raw_datasets(self, dir_path: str) -> Dict[str, BaseJSONDataset]:
        """Initialize raw datasets."""
        return {
            'business': RawBusiness(self.spark, dir_path),
            'checkin': RawCheckin(self.spark, dir_path),
            'review': RawReview(self.spark, dir_path),
            'tip': RawTip(self.spark, dir_path),
            'user': RawUser(self.spark, dir_path),
        }

    def init_clean_datasets(self, dir_path: str) -> Dict[str, BaseTableDataset]:
        """Initialize clean datasets."""
        return {
            'business': CleanBusiness(self.spark, dir_path),
            'checkin': CleanCheckin(self.spark, dir_path),
            'review': CleanReview(self.spark, dir_path),
            'tip': CleanTip(self.spark, dir_path),
            'user': CleanUser(self.spark, dir_path),
        }

    def init_aggregated_datasets(self, dir_path: str) -> Dict[str, BaseTableDataset]:
        """Initialize aggregated datasets."""
        return {
            'stars_per_business': StarsPerBusiness(self.spark, dir_path),
            'checkins_per_stars': CheckinsPerStars(self.spark, dir_path),
        }



if __name__ == '__main__':
    # Instantiate the Application class, triggering the data processing pipeline
    Application()