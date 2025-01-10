from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

"""
DAG to extract data, upload to MinIO, and load into Snowflake Data Warehouse
"""

# Output name of the extracted file. This will be passed to each task
output_name = datetime.now().strftime("%Y%m%d")

# Schedule the DAG to run daily
schedule_interval = "@daily"
start_date = days_ago(1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="etl_pipeline",
    description="ETL Pipeline for data extraction, upload to MinIO, and load to Snowflake",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["ETL"],
) as dag:

    extract_data = BashOperator(
        task_id="extract_data",
        bash_command=f"python /opt/airflow/scripts/extract_data.py {output_name}",
        dag=dag,
    )
    extract_data.doc_md = "Extract data and store locally as a CSV file"

    upload_to_minio = BashOperator(
        task_id="upload_to_minio",
        bash_command=f"python /opt/airflow/scripts/load_to_data_lake.py {output_name}",
        dag=dag,
    )
    upload_to_minio.doc_md = "Upload the extracted CSV file to MinIO"

    load_to_dwh = BashOperator(
        task_id="load_to_dwh",
        bash_command=f"python /opt/airflow/scripts/load_to_dwh.py {output_name}",
        dag=dag,
    )
    load_to_dwh.doc_md = "Load data from MinIO to Snowflake Data Warehouse"

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_snowflake && dbt run",
        dag=dag,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_snowflake && dbt test",
        dag=dag,
    )

    # Define task dependencies
    extract_data >> upload_to_minio >> load_to_dwh >> dbt_run >> dbt_test
