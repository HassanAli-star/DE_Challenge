import pandas as pd
import logging
import sys

# Custom exception for primary key violations
class PrimaryKeyViolationException(Exception):
    """Exception raised for primary key violations due to null, empty, or duplicate values."""
    pass


class DataValidator:
    """
    A utility class for validating data integrity in pandas DataFrames.
    """

    @staticmethod
    def check_empty_or_null_and_raise(df: pd.DataFrame, column_name: str):
        """
        Checks if a specific column in a DataFrame contains any empty or null (NaN) values.
        If such values are found, raises a PrimaryKeyViolationException, logs the error, and exits.

        Args:
            df (pd.DataFrame): The DataFrame to check.
            column_name (str): The name of the column to inspect.

        Raises:
            PrimaryKeyViolationException: If the column contains null or empty values.
            KeyError: If the column does not exist in the DataFrame.
        """
        # Configure logging
        logging.basicConfig(
            level=logging.ERROR,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]  # Log to console
        )
        logger = logging.getLogger(__name__)

        try:
            # Check if the column exists
            if column_name not in df.columns:
                raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")

            # Check for null (NaN) values
            is_null = df[column_name].isnull().any()

            # Check for empty strings if the column contains strings
            if df[column_name].dtype == 'object':
                is_empty = (df[column_name] == '').any()
            else:
                is_empty = False

            # If null or empty values are found, raise an exception
            if is_null or is_empty:
                error_message = f"Primary key violation: Column '{column_name}' contains null or empty values."
                logger.error(error_message)  # Log the error
                raise PrimaryKeyViolationException(error_message)

            # Log success if no issues found
            logger.info(f"Column '{column_name}' passed validation with no null or empty values.")

        except Exception as e:
            # Log the exception and exit
            logger.exception(f"Validation failed: {e}")
            sys.exit(1)

    @staticmethod
    def check_duplicates_and_raise(df: pd.DataFrame, column_name: str):
        """
        Checks if a specific column in a DataFrame contains duplicate values.
        If duplicates are found, raises a PrimaryKeyViolationException, logs the error, and exits.

        Args:
            df (pd.DataFrame): The DataFrame to check.
            column_name (str): The name of the column to inspect.

        Raises:
            PrimaryKeyViolationException: If duplicates are found in the column.
            KeyError: If the column does not exist in the DataFrame.
        """
        # Configure logging
        logging.basicConfig(
            level=logging.ERROR,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]  # Log to console
        )
        logger = logging.getLogger(__name__)

        try:
            # Check if the column exists
            if column_name not in df.columns:
                raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")

            # Check for duplicates
            duplicates = df[column_name].duplicated().any()

            if duplicates:
                error_message = f"Primary key violation: Column '{column_name}' contains duplicate values."
                logger.error(error_message)  # Log the error
                raise PrimaryKeyViolationException(error_message)

            # Log success if no duplicates found
            logger.info(f"Column '{column_name}' passed validation with no duplicate values.")

        except Exception as e:
            # Log the exception and exit
            logger.exception(f"Validation failed: {e}")
            sys.exit(1)
