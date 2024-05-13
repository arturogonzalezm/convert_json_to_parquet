from src.utilities.custom_exceptions import ETLJobError
from src.utilities.logger import log_error, log_info


class ETLJob:
    @staticmethod
    def extract_json(spark, input_file_path):
        try:
            return spark.read.json(input_file_path)
        except Exception as e:
            log_error(f"Failed to extract data: {e}")
            raise ETLJobError("Failed to extract data") from e

    @staticmethod
    def transform(df):
        try:
            rename_headers = [col.upper() for col in df.columns]
            return df.toDF(*rename_headers)
        except Exception as e:
            log_error(f"Failed to transform data: {e}")
            raise ETLJobError("Failed to transform data") from e

    @staticmethod
    def load(df, output_file_path):
        try:
            df.write.parquet(output_file_path)
            log_info(f"Data successfully written to {output_file_path}")
        except Exception as e:
            log_error(f"Failed to load data: {e}")
            raise ETLJobError("Failed to load data") from e

    @staticmethod
    def run(spark, input_file_path_json, output_file_path_parquet):
        try:
            df = ETLJob.extract_json(spark, input_file_path_json)
            df_transformed = ETLJob.transform(df)
            ETLJob.load(df_transformed, output_file_path_parquet)
            log_info(f"Data successfully read from {input_file_path_json} and written to {output_file_path_parquet}")
        except Exception as e:
            log_error(f"ETL job failed: {e}")
            raise ETLJobError("ETL job failed") from e
