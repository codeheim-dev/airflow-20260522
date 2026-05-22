from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import pendulum
import logging


def get_data_from_db(**context) -> None:
    from random import randint

    ti = context["ti"]

    hook = PostgresHook(postgres_conn_id="postgres-local")
    query = """
        SELECT usename AS role_name,
            CASE
            WHEN usesuper AND usecreatedb THEN
	            CAST('superuser, create database' AS pg_catalog.text)
            WHEN usesuper THEN
	            CAST('superuser' AS pg_catalog.text)
            WHEN usecreatedb THEN
	            CAST('create database' AS pg_catalog.text)
            ELSE
	            CAST('' AS pg_catalog.text)
            END role_attributes
        FROM pg_catalog.pg_user
        ORDER BY role_name desc;
"""

    filename = f"users_{randint(1000, 9999)}.parquet"

    df = hook.get_pandas_df(query)
    logger = logging.getLogger(__name__)
    logger.info("Get data from database")
    df.to_parquet(f"/home/student/airflow-20260522/data/parquet/{filename}")
    ti.xcom_push(key="filename", value=filename)
    logger.info(f"Save data to file: {filename}")


# Tworzenie DAG-a
with DAG(
    dag_id="hook_and_upload",
    description="SQL i Sensory",
    start_date=pendulum.datetime(2026, 5, 22, 8, 0),
    end_date=pendulum.datetime(2026, 5, 31, 0, 0),
    schedule=None,
    catchup=False,
    tags=["Airflow 3.x", "PythonOperator"],
) as dag:
    if_available = SqlSensor(
        task_id="if_available",
        mode="poke",  # dostępne opcje: reschedule
        poke_interval=pendulum.duration(seconds=15),
        timeout=pendulum.duration(minutes=1),
        conn_id="postgres-local",
        sql="SELECT 1",
    )

    save_users = PythonOperator(
        task_id="save_users",
        python_callable=get_data_from_db
    )

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        aws_conn_id="rustfs-local",
        filename="/home/student/airflow-20260522/data/parquet/{{ ti.xcom_pull(key='filename', task_ids='save_users') }}",
        dest_key="{{ ti.xcom_pull(key='filename', task_ids='save_users') }}",
        dest_bucket="postgres-data"
    )

    if_available >> save_users >> upload_to_s3
