class ETLJob:
    @staticmethod
    def extract_json(spark, input_file_path):
        return spark.read.json(input_file_path)

    @staticmethod
    def transform(df):
        rename_headers = [col.upper() for col in df.columns]
        return df.toDF(*rename_headers)

    @staticmethod
    def load(df, output_file_path):
        df.write.parquet(output_file_path)

    @staticmethod
    def run(spark, input_file_path_json, output_file_path_parquet):
        df = ETLJob.extract_json(spark, input_file_path_json)
        df_transformed = ETLJob.transform(df)
        ETLJob.load(df_transformed, output_file_path_parquet)
