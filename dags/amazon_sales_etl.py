from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="amazon_sales_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["amazon", "etl"]
) as dag:

    # create_product_summary = PostgresOperator(
    #     task_id="create_product_summary",
    #     postgres_conn_id="postgres_default",
    #     sql="create_product_summary.sql",
    # )

    # create_category_summary = PostgresOperator(
    #     task_id="create_category_summary",
    #     postgres_conn_id="postgres_default",
    #     sql="create_category_summary.sql",
    # )

    # create_top_product_summary = PostgresOperator(
    #     task_id="create_top_product_summary",
    #     postgres_conn_id="postgres_default",
    #     sql="create_top_product_summary.sql",
    # )

    # create_discount_effectiveness = PostgresOperator(
    #     task_id="create_discount_effectiveness",
    #     postgres_conn_id="postgres_default",
    #     sql="create_discount_effectiveness.sql",
    # )

    # create_product_summary >> create_category_summary >> create_top_product_summary >> create_discount_effectiveness
    
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="python /opt/airflow/spark/jobs/extract.py"
    )
    
    extract_data
    
    # transform_data = BashOperator(
    #     task_id="transform_data",
    #     bash_command="python /opt/airflow/spark/jobs/silver.py"
    # )
    
    # transform_data
    
    # gold_layer = BashOperator(
    #     task_id="gold_aggregations",
    #     bash_command="python /opt/airflow/spark/jobs/gold.py"
    # )
    
    # gold_layer
