"""
This module contains the ETLJob class, which performs ETL (Extract, Transform, Load) operations.
"""

from .utilities.custom_exceptions import ETLJobError
from .utilities.logger import log_error, log_info


class ETLJob:
    """
    A class to perform ETL (Extract, Transform, Load) operations.
    """

    @staticmethod
    def extract_json(spark, input_file_path):
        """
        Extract data from a JSON file using Spark.

        Args:
        - spark (SparkSession): The Spark session.
        - input_file_path (str): The path to the input JSON file.

        Returns:
        - DataFrame: The extracted data as a DataFrame.
        """
        try:
            return spark.read.json(input_file_path)
        except Exception as e:
            log_error(f"Failed to extract data: {e}")
            raise ETLJobError("Failed to extract data") from e

    @staticmethod
    def transform(df):
        """
        Transform the DataFrame by renaming column headers to uppercase.

        Args:
        - df (DataFrame): The DataFrame to transform.

        Returns:
        - DataFrame: The transformed DataFrame.
        """
        try:
            rename_headers = [col.upper() for col in df.columns]
            return df.toDF(*rename_headers)
        except Exception as e:
            log_error(f"Failed to transform data: {e}")
            raise ETLJobError("Failed to transform data") from e

    @staticmethod
    def load(df, output_file_path):
        """
        Load the DataFrame into a Parquet file.

        Args:
        - df (DataFrame): The DataFrame to load.
        - output_file_path (str): The path to the output Parquet file.
        """
        try:
            df.write.parquet(output_file_path)
            log_info(f"Data successfully written to {output_file_path}")
        except Exception as e:
            log_error(f"Failed to load data: {e}")
            raise ETLJobError("Failed to load data") from e

    @staticmethod
    def run(spark, input_file_path_json, output_file_path_parquet):
        """
        Run the ETL job.

        Args:
        - spark (SparkSession): The Spark session.
        - input_file_path_json (str): The path to the input JSON file.
        - output_file_path_parquet (str): The path to the output Parquet file.
        """
        try:
            df = ETLJob.extract_json(spark, input_file_path_json)
            df_transformed = ETLJob.transform(df)
            ETLJob.load(df_transformed, output_file_path_parquet)
            log_info(f"Data successfully read from {input_file_path_json} and written to {output_file_path_parquet}")
        except Exception as e:
            log_error(f"ETL job failed: {e}")
            raise ETLJobError("ETL job failed") from e
