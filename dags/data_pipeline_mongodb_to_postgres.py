from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import yaml
from etl.extract_data import *
from etl.transformation import *
from etl.load_data import *

env = 'DEV' # Environment can get through some enviroment vaiable
# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="data_pipeline_dag",
    default_args=default_args,
    description="Run ETL pipeline for each collection in sequence",
    schedule_interval=None,  # Trigger manually or set a cron schedule
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    def run_etl(collection_name):
        """
        Function to execute ETL logic for the given collection name.
        """
        try:
            # Print current working directory
            current_directory = os.getcwd()
            print(f"Current working directory: {current_directory}")

            # Load configuration
            config_path = "/opt/airflow/dags/etl/config.yml"
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)

            print(f"Starting ETL for collection: {collection_name}")

            # Create PostgreSQL engine
            engine = create_engine(
                f"postgresql://{config[env]['postgres']['user']}:{config[env]['postgres']['password']}@"
                f"{config[env]['postgres']['host']}:{config[env]['postgres']['port']}/{config[env]['postgres']['database']}"
            )
            # Initialize Loader
            loader = Loader()

            # Create tables
            loader.create_tables_from_yaml(engine, config[env]["postgres"]["schema_name"], collection_name, config.get("table_schemas", {collection_name}).get(collection_name, None))

            # Extract data
            source_type = "file"
            file_path = f"/opt/airflow/dags/input_data/{collection_name}.json"
            if collection_name == 'supplier_group':
                file_path = f"/opt/airflow/dags/input_data/clients.json"
            
            extractor = Extractor()
            data = extractor.read_data(source_type, file_path, config["input_column_names"][collection_name])

            # Transform data
            transformed_df = TransformationFactory.transform(data, case=collection_name)

            # Load data into the database
            loader.validate_and_insert_data(
                engine,
                transformed_df,
                collection_name,
                config[env]["postgres"]["schema_name"],
                config["output_column_names"][collection_name],
            )

            print(f"ETL process for collection '{collection_name}' completed successfully.")
        except Exception as e:
            raise RuntimeError(f"Error during ETL for collection '{collection_name}': {str(e)}")

    # Define tasks for each collection
    clients_task = PythonOperator(
        task_id="process_clients",
        python_callable=run_etl,
        op_args=["clients"],
    )
    
    supplier_group_task = PythonOperator(
        task_id="process_supplier_group",
        python_callable=run_etl,
        op_args=["supplier_group"],
    )

    suppliers_task = PythonOperator(
        task_id="process_suppliers",
        python_callable=run_etl,
        op_args=["suppliers"],
    )

    sonar_runs_task = PythonOperator(
        task_id="process_sonar_runs",
        python_callable=run_etl,
        op_args=["sonar_runs"],
    )

    sonar_results_task = PythonOperator(
        task_id="process_sonar_results",
        python_callable=run_etl,
        op_args=["sonar_results"],
    )

    # Set task dependencies
    clients_task >> supplier_group_task >> suppliers_task >> sonar_runs_task >> sonar_results_task
