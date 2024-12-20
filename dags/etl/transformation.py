import pandas as pd
from typing import Any
import logging
import sys
import yaml
from etl.data_validations.data_validator import *
from etl.data_quality_checks.data_quality import *

class TransformationFactory:
    """
    Factory class for handling data transformations based on specific cases.
    """
    
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Load column mappings from YAML
    with open("/opt/airflow/dags/etl/column_mappings.yml", "r") as file:
        column_mappings = yaml.safe_load(file)["column_mappings"]
    with open("/opt/airflow/dags/etl/column_mappings.yml", "r") as file:
        schema_definitions = yaml.safe_load(file)["schema_definitions"]

    @staticmethod
    def rename_columns(df: pd.DataFrame, case: str) -> pd.DataFrame:
        """
        Renames columns dynamically based on the specified case using mappings from the YAML file.

        Args:
            df (pd.DataFrame): The DataFrame to rename columns for.
            case (str): The transformation case ('clients', 'suppliers', etc.).

        Returns:
            pd.DataFrame: DataFrame with renamed columns.
        """
        mappings = TransformationFactory.column_mappings[case].get("rename", {})
        if mappings:
            df.rename(columns=mappings, inplace=True)
        return df

    @staticmethod
    def process_dict_columns(df, column_names):
        """
        Processes specified columns in a DataFrame that contain lists of dictionaries,
        detects the key(s), and converts them into lists of values if applicable.
    
        Args:
            df (pd.DataFrame): The DataFrame to process.
            column_names (list): List of column names to process.
    
        Returns:
            pd.DataFrame: DataFrame with the specified columns processed.
        """
        
        def extract_values(value):
            """
            Extracts values from a list of dictionaries if there is only one key.
            """
            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                # Find all keys in the first dictionary
                keys = set().union(*(d.keys() for d in value))
                
                # If there's only one key, return a list of values for that key
                if len(keys) == 1:
                    key = list(keys)[0]
                    return [item[key] for item in value]
            return value  # Return the original value if the format does not match
    
        # Process each specified column
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = df[column_name].apply(extract_values)
            else:
                raise KeyError(f"Column '{column_name}' not found in DataFrame.")
        return df

    @staticmethod
    def transform_clients(data: list) -> pd.DataFrame:
        """
        Transforms raw clients data into a DataFrame with cleaned columns.
        """
        df = pd.json_normalize(data)
        # rename input columns to the output columns
        df = TransformationFactory.rename_columns(df, "clients")
        # process the dict column like id and dates
        df = TransformationFactory.process_dict_columns(df, ["suppliers", "sonar_dates"])
        # create list of columns for select from column_mapping.yml file
        select_columns = TransformationFactory.column_mappings["clients"]["select"]
        if select_columns:
            client_df = df[eval(select_columns)]
        try:
            # Check the 'id' column for null, empty or duplicate values
            DataValidator.check_empty_or_null_and_raise(client_df, "client_id")
            DataValidator.check_duplicates_and_raise(client_df, "client_id")
            # Check data quality of DataFrame
            dq = DataQuality(TransformationFactory.schema_definitions)
            dq.check_data_quality_dataframe(client_df, "clients")
        except SystemExit as e:
            print(f"System exited with code {e.code}.")
         
        TransformationFactory.logger.info("Clients data transformed successfully.")
        return client_df

    @staticmethod
    def transform_supplier_group(data: dict) -> pd.DataFrame:
        """
        Transforms supplier_group data into a flattened DataFrame.
        """
        df = pd.DataFrame(data)
        # rename input columns to the output columns
        df = TransformationFactory.rename_columns(df,  "supplier_group")
        rows = []
        for _, row in df[["client_id", "supplier_groups"]].iterrows():
            supplier_groups = row["supplier_groups"]
            if supplier_groups:  # Check if the dictionary is not empty
                for group_name, suppliers in supplier_groups.items():  # Iterate over groups
                    for supplier in suppliers:  # Iterate over supplier list
                        rows.append({
                            'client_id': row['client_id'],
                            'group_name': group_name,
                            'supplier_id': supplier['$oid']  # Extract each supplier_id
                        })
        df = pd.DataFrame(rows)
        # Preprocess the id column to extract the '$oid' value
        df['client_id'] = df['client_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) and '$oid' in x else None)
        TransformationFactory.logger.info("Supplier group data transformed successfully.")
        return df

    @staticmethod
    def transform_suppliers(data: list) -> pd.DataFrame:
        """
        Transforms raw clients data into a DataFrame with cleaned columns.
        """
        df = pd.json_normalize(data)
        # rename input columns to the output columns
        df = TransformationFactory.rename_columns(df,  "suppliers")
        # create list of columns for select from column_mapping.yml file
        select_columns = TransformationFactory.column_mappings["suppliers"]["select"]
        if select_columns:
            supplier_df = df[eval(select_columns)]
        try:
            # Check the 'id' column for null or empty values
            DataValidator.check_empty_or_null_and_raise(supplier_df, "supplier_id")
            DataValidator.check_duplicates_and_raise(supplier_df, "supplier_id")
            # Check data quality of DataFrame
            dq = DataQuality(TransformationFactory.schema_definitions)
            dq.check_data_quality_dataframe(supplier_df, "suppliers")
        except SystemExit as e:
            print(f"System exited with code {e.code}.")
        TransformationFactory.logger.info("Clients data transformed successfully.")
        return supplier_df

    @staticmethod
    def transform_sonar_runs(data: list) -> pd.DataFrame:
        """
        Transforms raw clients data into a DataFrame with cleaned columns.
        """
        df = pd.json_normalize(data)
        # Convert the column to a flattened array of strings, handling None/NaN
        df["supplier_ids"] = df["supplier_ids"].apply(
            lambda x: [item["$oid"] for item in x] if isinstance(x, list) else []
        )
        df["client_part_ids"] = df["client_part_ids"].apply(
            lambda x: [item["$oid"] for item in x] if isinstance(x, list) else []
        )
        # rename columns into output column
        df = TransformationFactory.rename_columns(df,  "sonar_runs")
        # create list of columns for select from column_mapping.yml file
        select_columns = TransformationFactory.column_mappings["sonar_runs"]["select"]
        if select_columns:
            sonar_run_df = df[eval(select_columns)]
        try:
            # Check the 'id' column for null or empty values
            DataValidator.check_empty_or_null_and_raise(sonar_run_df, "sonar_run_id")
            DataValidator.check_duplicates_and_raise(sonar_run_df, "sonar_run_id")
            # Check data quality of DataFrame
            dq = DataQuality(TransformationFactory.schema_definitions)
            dq.check_data_quality_dataframe(sonar_run_df, "sonar_runs")
        except SystemExit as e:
            print(f"System exited with code {e.code}.")
        TransformationFactory.logger.info("Clients data transformed successfully.")
        return sonar_run_df

    @staticmethod
    def transform_sonar_results(data: list) -> pd.DataFrame:
        """
        Transforms raw clients data into a DataFrame with cleaned columns.
        """
        df = pd.json_normalize(data)
        # rename columns into output column
        df = TransformationFactory.rename_columns(df,  "sonar_results")
        # create list of columns for select from column_mapping.yml file
        select_columns = TransformationFactory.column_mappings["sonar_results"]["select"]
        if select_columns:
            sonar_result_df = df[eval(select_columns)]
        try:
            # Check the 'id' column for null or empty values
            DataValidator.check_empty_or_null_and_raise(sonar_result_df, "sonar_result_id")
            DataValidator.check_duplicates_and_raise(sonar_result_df, "sonar_result_id")
            # Check data quality of DataFrame
            dq = DataQuality(TransformationFactory.schema_definitions)
            dq.check_data_quality_dataframe(sonar_result_df, "sonar_results")
        except SystemExit as e:
            print(f"System exited with code {e.code}.")
        TransformationFactory.logger.info("Clients data transformed successfully.")
        return sonar_result_df

    @classmethod
    def transform(cls, data: Any, case: str) -> pd.DataFrame:
        """
        Returns the appropriate transformed DataFrame based on the case.

        Args:
            data (Any): Input data to be transformed.
            case (str): The transformation case ('clients', 'supplier_group', etc.).

        Returns:
            pd.DataFrame: Transformed DataFrame.

        Raises:
            ValueError: If the case is not recognized.
        """
        if case == "clients":
            return cls.transform_clients(data)
        elif case == "supplier_group":
            return cls.transform_supplier_group(data)
        elif case == "suppliers":
            return cls.transform_suppliers(data)
        elif case == "sonar_runs":
            return cls.transform_sonar_runs(data)
        elif case == "sonar_results":
            return cls.transform_sonar_results(data)
        else:
            raise ValueError(f"Unknown transformation case: {case}")






