import pytest
from unittest.mock import MagicMock, patch
from src.utilities.custom_exceptions import ETLJobError
from src.etl import ETLJob


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
