import pandas as pd
import logging
from typing import Dict


class DataQuality:
    """
    A utility class to validate the quality of data in a DataFrame based on schema definitions.
    """

    def __init__(self, schema_definitions: Dict):
        """
        Initializes the DataQuality object with schema definitions.

        Args:
            schema_definitions (Dict): A dictionary of table schemas loaded from YAML.
        """
        self.schema_definitions = schema_definitions
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def validate_date_column(self, df: pd.DataFrame, column_name: str):
        """
        Validates that all values in a date column are valid datetime objects or strings.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            column_name (str): The name of the column to validate.

        Raises:
            ValueError: If invalid dates are found.
        """
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")

        invalid_dates = df[column_name].apply(
            lambda x: pd.to_datetime(x, errors="coerce")
        ).isnull()

        if invalid_dates.any():
            self.logger.error(f"Invalid dates found in column '{column_name}'.")
            raise ValueError(f"Invalid dates found in column '{column_name}'.")

        self.logger.info(f"Date column '{column_name}' passed validation.")

    def validate_id_column(self, df: pd.DataFrame, column_name: str, max_length: int):
        """
        Validates that all values in an ID column are non-null and have a maximum length.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            column_name (str): The name of the column to validate.
            max_length (int): Maximum allowed length for the column.

        Raises:
            ValueError: If invalid IDs are found.
        """
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")

        null_or_empty = df[column_name].isnull() | (df[column_name] == "")
        if null_or_empty.any():
            self.logger.error(f"Null or empty values found in ID column '{column_name}'.")
            raise ValueError(f"Null or empty values found in ID column '{column_name}'.")

        invalid_length = df[column_name].apply(lambda x: len(str(x)) > max_length)
        if invalid_length.any():
            self.logger.error(f"Values exceeding {max_length} characters found in ID column '{column_name}'.")
            raise ValueError(f"Values exceeding {max_length} characters found in ID column '{column_name}'.")
            
        invalid_length = df[column_name].apply(lambda x: len(str(x)) < max_length)
        if invalid_length.any():
            self.logger.error(f"Values less than {max_length} characters found in ID column '{column_name}'.")
            raise ValueError(f"Values less than {max_length} characters found in ID column '{column_name}'.")

        self.logger.info(f"ID column '{column_name}' passed validation.")

    def validate_column_types(self, df: pd.DataFrame, column_name: str, expected_type: str):
        """
        Validates the data type of a column against an expected type.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            column_name (str): The name of the column to check.
            expected_type (str): The expected data type ('float', 'boolean', etc.).

        Raises:
            ValueError: If any value in the column does not match the expected type.
        """
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")

        if expected_type == "float":
            # Replace Null with 0 
            df[column_name] = df[column_name].fillna(0)
            if not pd.to_numeric(df[column_name], errors="coerce").notnull().all():
                self.logger.error(f"Column '{column_name}' contains non-float values.")
                raise ValueError(f"Column '{column_name}' contains non-float values.")

        elif expected_type == "boolean":
            if not df[column_name].isin([True, False, 1, 0]).all():
                self.logger.error(f"Column '{column_name}' contains non-boolean values.")
                raise ValueError(f"Column '{column_name}' contains non-boolean values.")

        elif expected_type == "varchar":
            self.logger.warning(f"Use 'validate_id_column' for varchar validations with length.")

        self.logger.info(f"Column '{column_name}' passed type validation for {expected_type}.")

    def check_data_quality_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Validates a DataFrame based on schema definitions for a given table.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            table_name (str): The name of the table whose schema is used for validation.

        Raises:
            ValueError: If validation fails for any column.
        """
        schema = self.schema_definitions.get(table_name)
        if not schema:
            raise ValueError(f"No schema found for table '{table_name}'.")

        for column_def in schema:
            column_name = column_def["name"]
            column_type = column_def["type"].lower()

            # Validate dates
            if "date" in column_type:
                self.validate_date_column(df, column_name)

            # Validate IDs
            if column_type == "varchar" and "length" in column_def:
                self.validate_id_column(df, column_name, column_def["length"])

            # Validate floats and booleans
            if column_type in ["float", "boolean"]:
                self.validate_column_types(df, column_name, column_type)

        self.logger.info(f"All validations passed for table '{table_name}'.")
