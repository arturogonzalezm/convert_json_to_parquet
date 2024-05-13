"""
Test logger module.
"""

from unittest.mock import patch

from src.utilities.logger import log_error, log_info


def test_log_error():
    """
    Test log_error function.
    """
    with patch('src.utilities.logger.logging.error') as mock_logging_error:
        # Call log_error function
        log_error("Error message")

        # Assert that logging.error was called with the correct message
        mock_logging_error.assert_called_once_with("Error message")


def test_log_info():
    """
    Test log_info function.
    """
    with patch('src.utilities.logger.logging.info') as mock_logging_info:
        # Call log_info function
        log_info("Info message")

        # Assert that logging.info was called with the correct message
        mock_logging_info.assert_called_once_with("Info message")
