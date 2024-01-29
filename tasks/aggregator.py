from typing import Dict

from nycs.datasets.base.BaseTableDataset import BaseTableDataset
from nycs.transformers.data_aggregator import DataAggregator


class Aggregator:
    """
    Aggregator class for transforming and aggregating datasets.

    Parameters:
    - clean_datasets (Dict[str, BaseTableDataset]): Dictionary of clean datasets.
    - agg_datasets (Dict[str, BaseTableDataset]): Dictionary of aggregated datasets.
    """

    def __init__(self, clean_datasets: Dict[str, BaseTableDataset], agg_datasets: Dict[str, BaseTableDataset]):
        """
        Constructor for the Aggregator class.

        Parameters:
        - clean_datasets (Dict[str, BaseTableDataset]): Dictionary of clean datasets.
        - agg_datasets (Dict[str, BaseTableDataset]): Dictionary of aggregated datasets.
        """
        self.clean_datasets = clean_datasets
        self.agg_datasets = agg_datasets

    def transform(self):
        """
        Transforms and aggregates clean datasets to create aggregated datasets.

        This method uses the DataAggregator class to generate aggregated dataframes based on the clean business
        and checkin datasets. The resulting aggregated datasets are then saved using the `save` method.

        Note: The aggregated datasets include 'stars_per_business' and 'checkins_per_stars'.

        Returns:
        - None
        """
        # Aggregate stars per business and checkins per stars
        self.agg_datasets['stars_per_business'].df = (
            DataAggregator.get_stars_per_business(self.clean_datasets['business'].df,
                                                  self.clean_datasets['checkin'].df))
        self.agg_datasets['checkins_per_stars'].df = (
            DataAggregator.get_checkins_per_stars(self.clean_datasets['business'].df,
                                                  self.clean_datasets['checkin'].df))
        # Save the aggregated datasets
        for name, _ in self.agg_datasets.items():
            self.agg_datasets[name].save_as_csv()
