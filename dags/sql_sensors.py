from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum


# Tworzenie DAG-a
with DAG(
    dag_id="sql_sensors",
    description="SQL i Sensory",
    start_date=pendulum.datetime(2026, 5, 22, 8, 0),
    end_date=pendulum.datetime(2026, 5, 31, 0, 0),
    schedule=None,
    catchup=False,
    tags=["Airflow 3.x", "SqlSensor"]
) as dag:

    if_available = SqlSensor(
        task_id="if_available",
        mode="poke",    # dostępne opcje: reschedule
        poke_interval=pendulum.duration(seconds=15),
        timeout=pendulum.duration(minutes=1),
        conn_id="postgres-local",
        sql="SELECT 1"
    )

    get_version = SQLExecuteQueryOperator(
        task_id="get_version",
        conn_id="postgres-local",
        sql="SELECT version();"
    )

    if_available >> get_version