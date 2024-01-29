from pyspark.sql import DataFrame


class DataAggregator:
    """
    DataAggregator class for aggregating and computing metrics on business and checkin data.

    Methods:
    - get_stars_per_business(business_df: DataFrame, checkin_df: DataFrame) -> DataFrame:
      Aggregates the average stars per business on a weekly basis.

    - get_checkins_per_stars(business_df: DataFrame, checkin_df: DataFrame) -> DataFrame:
      Aggregates the average stars and checkin count per business.
    """
    @staticmethod
    def get_stars_per_business(business_df: DataFrame, checkin_df: DataFrame) -> DataFrame:
        """
        Aggregates the average stars per business on a weekly basis.

        Parameters:
        - business_df (DataFrame): DataFrame containing business data.
        - checkin_df (DataFrame): DataFrame containing checkin data.

        Returns:
        - DataFrame: Aggregated DataFrame with columns 'business_id', 'week', and 'avg_stars_per_week'.
        """
        return (
            business_df
            .join(checkin_df, "business_id", "left")
            .groupBy("business_id", "week")
            .agg({"stars": "avg"})
            .withColumnRenamed("avg(stars)", "avg_stars_per_week")
        )

    @staticmethod
    def get_checkins_per_stars(business_df: DataFrame, checkin_df: DataFrame) -> DataFrame:
        """
        Aggregates the average stars and checkin count per business.

        Parameters:
        - business_df (DataFrame): DataFrame containing business data.
        - checkin_df (DataFrame): DataFrame containing checkin data.

        Returns:
        - DataFrame: Aggregated DataFrame with columns 'business_id', 'avg_stars', and 'checkin_count'.
        """
        return (
            business_df
            .join(checkin_df, "business_id", "left")
            .groupBy("business_id")
            .agg({"stars": "avg", "date": "count"})
            .withColumnRenamed("avg(stars)", "avg_stars")
            .withColumnRenamed("count(date)", "checkin_count")
        )
