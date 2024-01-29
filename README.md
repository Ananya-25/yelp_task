# Yelp Data Lake - Data Engineering Exercise

This project is a demonstration of data engineering skills using the Yelp JSON dataset and PySpark. The objective is to create a small data lake, consisting of raw, cleaned, and aggregated Yelp data.

### Project Structure

The project follows a [medallion architecture](https://www.databricks.com/glossary/medallion-architecture), organizing data into three main folders:

- **dataset/bronze**: Contains raw data.
- **dataset/silver**: Stores clean data.
- **dataset/gold**: Holds aggregated data.

The overall structure of the project includes dedicated folders for different stages of data processing:

- **base**: Manages read and write operations for JSON and tables.
- **tasks**: Contains the following modules:
  - **preprocessor**: Cleans raw datasets, removes NaNs and duplicates, and augments data.
  - **aggregator**: Aggregates stars per business on a weekly basis and checks the number of check-ins compared to the overall star rating.
- **tests**: Holds unit tests.
- **transformers**: Includes the following modules:
  - **data_cleaner**: Checks NaNs and duplicates in dataframes.
  - **data_augmentor**: Adds additional columns for processing.
  - **data_aggregator**: Aggregates stars per business on a weekly basis and compares check-ins to overall star ratings.
- **data**: Stores raw, clean and aggregated data.
- **core**: Contains the `run.py` script which initializes Spark and manages the flow of raw, clean, and aggregated datasets throughout the project.

### How to run
1. **Download Yelp Dataset:**
   1. Use the following link to download the Yelp JSON dataset: [Yelp Dataset](https://www.yelp.com/dataset/download)
   2. Unzip the contents under the folder `nycs/data/bronze`
2. **Build Docker images**
   1. Navigate to root folder (by default yelp_task)
   2. Run `docker-compose build`
3. **Run the project and unit-tests**
   1. Run unit-tests `docker-compose run tests`
   2. Run main `docker-compose run main`
4. **Output**
    1. The ouput data will be written in CSV-format for readability into `nycs/data/gold/`

### Installation requirements
1. Python 3.10
2. Docker 

