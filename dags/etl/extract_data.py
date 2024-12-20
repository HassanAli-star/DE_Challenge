import json
import pandas as pd
from typing import List, Union
import logging


class Extractor:
    """
    A class for extracting and validating data from various sources (e.g., JSON files, MongoDB).
    """

    def __init__(self):
        """
        Initializes the Extractor with logging configuration.
        """
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def read_data(self, source_type: str, source: str, schema_definition: List[str]) -> pd.DataFrame:
        """
        Reads data from a JSON file or MongoDB collection and validates it against a schema definition.

        Args:
            source_type (str): 'file' for JSON file, 'mongo' for MongoDB.
            source (str): File path for JSON file or MongoDB URI.
            schema_definition (List[str]): List of required columns to extract.

        Returns:
            pd.DataFrame: Filtered DataFrame containing only the specified columns.

        Raises:
            ValueError: If required columns are missing or invalid.
        """
        try:
            # Step 1: Load data
            if source_type == "file":
                data = self._read_from_file(source)
            elif source_type == "mongo":
                data = self._read_from_mongo(source)  # Placeholder for MongoDB implementation
            else:
                raise ValueError("Invalid source_type. Use 'file' or 'mongo'.")

            # Step 2: Convert to DataFrame
            df = pd.DataFrame(data)

            # Step 3: Validate schema
            self._validate_schema(df, schema_definition)

            # Step 4: Filter columns based on schema
            df = df[schema_definition]

            self.logger.info(f"Data successfully extracted and validated for schema: {schema_definition}")
            return data

        except FileNotFoundError:
            self.logger.error(f"File not found: {source}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON format in file: {source}")
            raise ValueError(f"Invalid JSON format in file: {source}")
        except ValueError as e:
            self.logger.error(f"Validation Error: {e}")
            raise
        except Exception as e:
            self.logger.exception(f"Error reading or processing data: {e}")
            raise

    def _read_from_file(self, source: str) -> List[dict]:
        """
        Reads data from a JSON file.

        Args:
            source (str): File path for JSON file.

        Returns:
            List[dict]: List of records read from the file.

        Raises:
            FileNotFoundError: If the file is not found.
            json.JSONDecodeError: If the file content is not valid JSON.
        """
        try:
            with open(source, "r", encoding="utf-8", errors="replace") as file:
                data = json.load(file)
            self.logger.info(f"Data successfully read from file: {source}")
            return data
        except Exception as e:
            self.logger.exception(f"Error reading file {source}: {e}")
            raise

    def _read_from_mongo(self, source: str) -> List[dict]:
        """
        Reads data from a MongoDB collection. (Placeholder implementation)

        Args:
            source (str): MongoDB URI or connection details.

        Returns:
            List[dict]: List of records read from MongoDB.

        Raises:
            NotImplementedError: If MongoDB reading is not implemented.
        """
        self.logger.warning("MongoDB extraction is not yet implemented.")
        raise NotImplementedError("MongoDB extraction is not yet implemented.")

    def _validate_schema(self, df: pd.DataFrame, schema_definition: List[str]):
        """
        Validates that all required columns in the schema definition exist in the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            schema_definition (List[str]): List of required columns.

        Raises:
            ValueError: If any required columns are missing.
        """
        missing_columns = [col for col in schema_definition if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        self.logger.info("Schema validation passed.")
