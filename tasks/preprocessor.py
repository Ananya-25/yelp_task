from typing import Dict

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset
from nycs.datasets.base.BaseTableDataset import BaseTableDataset
from nycs.transformers.data_augmentor import DataAugmentor
from nycs.transformers.data_cleaner import DataCleaner


class Preprocessor:
    """
    Preprocessor class for transforming and cleaning raw datasets.

    Parameters:
    - raw_datasets (Dict[str, BaseJSONDataset]): Dictionary of raw datasets.
    - clean_datasets (Dict[str, BaseTableDataset]): Dictionary of clean datasets.
    """

    def __init__(self, raw_datasets: Dict[str, BaseJSONDataset], clean_datasets: Dict[str, BaseTableDataset]):
        """
        Constructor for the Preprocessor class.

        Parameters:
        - raw_datasets (Dict[str, BaseJSONDataset]): Dictionary of raw datasets.
        - clean_datasets (Dict[str, BaseTableDataset]): Dictionary of clean datasets.
        """
        self.raw_datasets = raw_datasets
        self.clean_datasets = clean_datasets

    def transform(self):
        """
        Method for transforming and cleaning raw datasets.

        - Cleans each raw dataset by removing null values and duplicates.
        - Augments the 'checkin' dataset by adding a 'week_of_year' column.
        - Saves the cleaned datasets.
        """
        # clean datasets
        for name, data_cls in self.raw_datasets.items():
            df = data_cls.read()
            # remove na
            df = DataCleaner.remove_na(data_cls.mandatory_fields, df)
            # remove duplicates
            df = DataCleaner.remove_duplicates(data_cls.mandatory_fields, df)

            self.clean_datasets[name].df = df

        # augment datasets
        self.clean_datasets['checkin'].df = DataAugmentor.add_week_of_year(self.clean_datasets['checkin'].df,
                                                                           'date')
        # save data
        for name, _ in self.clean_datasets.items():
            self.clean_datasets[name].save()
