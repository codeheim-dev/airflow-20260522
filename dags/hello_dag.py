from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

# Tworzenie DAG-a
with DAG(
    dag_id="hello_dag",
    description="Przykładowy DAG Apache Airflow",
    start_date=pendulum.datetime(2026, 5, 22, 8, 0),
    end_date=pendulum.datetime(2026, 5, 31, 0, 0),
    schedule=None,
    catchup=False,
    tags=["EmptyOperator", "Airflow 3.x"]
) as dag:
    # Tworzenie zadań, sensorów
    start = EmptyOperator(
        task_id="start"
    )

    # Flow (kolejność wykonywania zadań)
    start