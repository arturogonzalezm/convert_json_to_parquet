"""
This module contains unit tests for the ETLJob class in src/etl_job.py
"""

import pytest
from unittest.mock import MagicMock, patch
from src.utilities.custom_exceptions import ETLJobError
from src.etl_job import ETLJob


@pytest.fixture
def spark():
    return MagicMock()


@pytest.fixture
def input_file_path():
    return "data/input/test.json"


@pytest.fixture
def output_file_path():
    return "data/output/test.parquet"


def test_extract_json_success(spark, input_file_path):
    # Mock successful data extraction
    expected_data = [("Alice", 30), ("Bob", 25)]
    mock_df = MagicMock()
    mock_df.columns = ["name", "age"]
    mock_df.count.return_value = 2
    mock_df.collect.return_value = expected_data
    spark.read.json.return_value = mock_df

    # Call extract_json method
    data_frame = ETLJob.extract_json(spark, input_file_path)

    # Assert the extracted data
    assert data_frame.count() == 2
    assert data_frame.collect() == expected_data


@patch('src.etl.log_error')
def test_extract_json_failure(mock_log_error, spark, input_file_path):
    # Mock data extraction failure
    spark.read.json.side_effect = Exception("Failed to read JSON")

    # Call extract_json method and expect an ETLJobError
    with pytest.raises(ETLJobError):
        ETLJob.extract_json(spark, input_file_path)

    mock_log_error.assert_called_once()


@patch('src.etl.log_error')
def test_transform_success(mock_log_error):
    # Mock DataFrame
    mock_df = MagicMock()
    mock_df.columns = ['col1', 'col2']

    # Call transform method
    transformed_df = ETLJob.transform(mock_df)

    # Assert that toDF was called with the correct arguments
    mock_df.toDF.assert_called_once_with('COL1', 'COL2')

    # Assert that the transformed DataFrame is returned
    assert transformed_df == mock_df.toDF.return_value

    mock_log_error.assert_not_called()


@patch('src.etl.log_error')
def test_transform_failure(mock_log_error):
    # Mock DataFrame
    mock_df = MagicMock()
    mock_df.columns = ['col1', 'col2']

    # Set side effect for toDF to simulate an exception
    mock_df.toDF.side_effect = Exception("Failed to transform data")

    # Call transform method and expect ETLJobError
    with pytest.raises(ETLJobError):
        ETLJob.transform(mock_df)

    # Assert that toDF was called with the correct arguments
    mock_df.toDF.assert_called_once_with('COL1', 'COL2')

    mock_log_error.assert_called_once()


@patch('src.etl.log_error')
@patch('src.etl.log_info')
def test_load_success(mock_log_info, mock_log_error):
    # Mock DataFrame
    mock_df = MagicMock()

    # Call load method
    ETLJob.load(mock_df, "data/output/test.parquet")

    # Assert that write.parquet was called with the correct arguments
    mock_df.write.parquet.assert_called_once_with("data/output/test.parquet")
    mock_log_info.assert_called_once()
    mock_log_error.assert_not_called()


@patch('src.etl.log_error')
@patch('src.etl.log_info')
def test_load_failure(mock_log_info, mock_log_error):
    # Mock DataFrame
    mock_df = MagicMock()

    # Set side effect for write.parquet to simulate an exception
    mock_df.write.parquet.side_effect = Exception("Failed to write parquet file")

    # Call load method and expect ETLJobError
    with pytest.raises(ETLJobError):
        ETLJob.load(mock_df, "data/output/test.parquet")

    # Assert that write.parquet was called with the correct arguments
    mock_df.write.parquet.assert_called_once_with("data/output/test.parquet")
    mock_log_error.assert_called_once()
    mock_log_info.assert_not_called()


@patch('src.etl.ETLJob.extract_json')
@patch('src.etl.ETLJob.transform')
@patch('src.etl.ETLJob.load')
@patch('src.etl.log_info')
@patch('src.etl.log_error')
def test_run_success(mock_log_error, mock_log_info, mock_load, mock_transform, mock_extract_json):
    # Mock objects
    spark = MagicMock()
    input_file_path_json = "data/input/test.json"
    output_file_path_parquet = "data/output/test.parquet"
    mock_df = MagicMock()

    # Set return values for mocked methods
    mock_extract_json.return_value = mock_df
    mock_transform.return_value = mock_df

    # Call run method
    ETLJob.run(spark, input_file_path_json, output_file_path_parquet)

    # Assert that extract_json, transform, and load were called with the correct arguments
    mock_extract_json.assert_called_once_with(spark, input_file_path_json)
    mock_transform.assert_called_once_with(mock_df)
    mock_load.assert_called_once_with(mock_df, output_file_path_parquet)
    mock_log_info.assert_called_once_with(
        f"Data successfully read from {input_file_path_json} and written to {output_file_path_parquet}")
    mock_log_error.assert_not_called()


@patch('src.etl.ETLJob.extract_json')
@patch('src.etl.ETLJob.transform')
@patch('src.etl.ETLJob.load')
@patch('src.etl.log_info')
@patch('src.etl.log_error')
def test_run_failure(mock_log_error, mock_log_info, mock_load, mock_transform, mock_extract_json):
    # Mock objects
    spark = MagicMock()
    input_file_path_json = "data/input/test.json"
    output_file_path_parquet = "data/output/test.parquet"

    # Set side effect for extract_json to simulate an exception
    mock_extract_json.side_effect = Exception("Failed to extract JSON")

    # Call run method and expect ETLJobError
    with pytest.raises(ETLJobError):
        ETLJob.run(spark, input_file_path_json, output_file_path_parquet)

    # Assert that extract_json was called with the correct arguments
    mock_extract_json.assert_called_once_with(spark, input_file_path_json)
    mock_transform.assert_not_called()
    mock_load.assert_not_called()
    mock_log_info.assert_not_called()
    mock_log_error.assert_called_once()
