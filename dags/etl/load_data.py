import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging
from typing import Dict, List


class Loader:
    """
    A class for managing the loading of data into PostgreSQL, including table creation and data insertion.
    """

    def __init__(self):
        """
        Initializes the Loader with logging configuration.
        """
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def create_tables_from_yaml(self, engine: Engine, schema_name: str, table_name: str, table_schema: str):
        """
        Creates tables in PostgreSQL using table creation commands from the YAML file.
        If the table already exists, it logs a message and skips creation.

        Args:
            engine (Engine): SQLAlchemy database engine.
            schema_name (str): Schema name, used to create the schema if it doesn't exist.
            table_name (str): Table name, used to log messages.
            table_schema (str): CREATE TABLE SQL command.

        Raises:
            Exception: If there's an error connecting to the database or creating the table.
        """
        try:
            with engine.connect() as connection:
                try:
                    # Ensure the schema exists
                    connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema_name};'))
                    self.logger.info(f"Schema '{schema_name}' created or already exists.")

                    # Create the table
                    connection.execute(text(table_schema))
                    self.logger.info(f"Table '{schema_name}.{table_name}' created successfully or already exists.")
                except Exception as e:
                    self.logger.error(f"Error creating table '{schema_name}.{table_name}': {e}")
                    raise
        except Exception as e:
            self.logger.exception(f"Error connecting to database or processing table creation: {e}")
            raise

    def validate_and_insert_data(self, engine: Engine, df: pd.DataFrame, table_name: str, schema_name: str, required_columns: List[str]):
        """
        Validates data and inserts it into PostgreSQL.

        Args:
            engine: SQLAlchemy engine for PostgreSQL.
            df: DataFrame containing the data.
            table_name: Name of the PostgreSQL table.
            schema_name: Schema name of the table.
            required_columns: List of required columns for validation.

        Raises:
            ValueError: If required columns are missing in the DataFrame.
            Exception: If there's an error inserting data into the table.
        """
        try:
            # Check for required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"Missing columns for table '{schema_name}.{table_name}': {missing_columns}")
                raise ValueError(f"Missing columns for table '{schema_name}.{table_name}': {missing_columns}")

            # Truncate the table before inserting data
            with engine.connect() as connection:
                truncate_command = f"TRUNCATE TABLE {schema_name}.{table_name} CASCADE;"
                connection.execute(text(truncate_command))
                self.logger.info(f"Table '{schema_name}.{table_name}' truncated successfully.")

            # Insert data into the table
            df.to_sql(table_name, engine, if_exists="append", schema=schema_name, index=False)
            self.logger.info(f"Data successfully inserted into table: {schema_name}.{table_name}")
        except ValueError as e:
            self.logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            self.logger.exception(f"Error inserting data into table '{schema_name}.{table_name}': {e}")
            raise
