from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from nycs.datasets.base.BaseJSONDataset import BaseJSONDataset


class RawBusiness(BaseJSONDataset):
    # Define class variables for mandatory fields and raw JSON filename
    mandatory_fields = ['business_id']  # List of mandatory fields in the "business" table
    raw_json_filename = "yelp_academic_dataset_business.json"  # Filename of the raw JSON data

    def __init__(self, spark, dir_path):
        """
        Constructor for RawBusiness.

        Parameters:
        - spark: PySpark session
        - dir_path: Directory path where the JSON data is stored
        """
        super().__init__(spark, dir_path)

    # Define the schema for the "business" table
    schema = StructType([
        StructField("address", StringType(), True),
        StructField("attributes", StructType([
            StructField("AcceptsInsurance", StringType(), True),
            StructField("AgesAllowed", StringType(), True),
            StructField("Alcohol", StringType(), True),
            StructField("Ambience", StringType(), True),
            StructField("BYOB", StringType(), True),
            StructField("BYOBCorkage", StringType(), True),
            StructField("BestNights", StringType(), True),
            StructField("BikeParking", StringType(), True),
            StructField("BusinessAcceptsBitcoin", StringType(), True),
            StructField("BusinessAcceptsCreditCards", StringType(), True),
            StructField("BusinessParking", StringType(), True),
            StructField("ByAppointmentOnly", StringType(), True),
            StructField("Caters", StringType(), True),
            StructField("CoatCheck", StringType(), True),
            StructField("Corkage", StringType(), True),
            StructField("DietaryRestrictions", StringType(), True),
            StructField("DogsAllowed", StringType(), True),
            StructField("DriveThru", StringType(), True),
            StructField("GoodForDancing", StringType(), True),
            StructField("GoodForKids", StringType(), True),
            StructField("GoodForMeal", StringType(), True),
            StructField("HairSpecializesIn", StringType(), True),
            StructField("HappyHour", StringType(), True),
            StructField("HasTV", StringType(), True),
            StructField("Music", StringType(), True),
            StructField("NoiseLevel", StringType(), True),
            StructField("Open24Hours", StringType(), True),
            StructField("OutdoorSeating", StringType(), True),
            StructField("RestaurantsAttire", StringType(), True),
            StructField("RestaurantsCounterService", StringType(), True),
            StructField("RestaurantsDelivery", StringType(), True),
            StructField("RestaurantsGoodForGroups", StringType(), True),
            StructField("RestaurantsPriceRange2", StringType(), True),
            StructField("RestaurantsReservations", StringType(), True),
            StructField("RestaurantsTableService", StringType(), True),
            StructField("RestaurantsTakeOut", StringType(), True),
            StructField("Smoking", StringType(), True),
            StructField("WheelchairAccessible", StringType(), True),
            StructField("WiFi", StringType(), True)
        ]), True),
        StructField("business_id", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("city", StringType(), True),
        StructField("hours", StructType([
            StructField("Friday", StringType(), True),
            StructField("Monday", StringType(), True),
            StructField("Saturday", StringType(), True),
            StructField("Sunday", StringType(), True),
            StructField("Thursday", StringType(), True),
            StructField("Tuesday", StringType(), True),
            StructField("Wednesday", StringType(), True)
        ]), True),
        StructField("is_open", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("review_count", LongType(), True),
        StructField("stars", DoubleType(), True),
        StructField("state", StringType(), True)
    ])
