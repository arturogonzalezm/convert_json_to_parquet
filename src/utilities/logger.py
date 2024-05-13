"""
This module contains functions for logging messages.
"""

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def log_error(message):
    """
    Log an error message.

    Args:
    - message (str): The error message to log.
    """
    logging.error(message)


def log_info(message):
    """
    Log an info message.

    Args:
    - message (str): The info message to log.
    """
    logging.info(message)

