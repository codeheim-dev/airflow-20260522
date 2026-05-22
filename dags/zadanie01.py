"""
Zadanie 01
DAG składa się z dwóch zadań:
    1. Generuje 5 liczb do pliku
    2. Pobiera liczby z pliku i oblicza ?redni?
"""

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

def get_avg_from(file: str = "/home/student/airflow-20260522/data/txt/numbers.txt") -> None:
    with open(file, "r") as f:
        numbers = [int(x) for x in f.read().split()] 
        print(sum(numbers) / len(numbers))

# Tworzenie DAG-a
with DAG(
    dag_id="zadanie01",
    description="Zadanie 01",
    start_date=pendulum.datetime(2026, 5, 22, 8, 0),
    end_date=pendulum.datetime(2026, 5, 31, 0, 0),
    schedule=None,
    catchup=False,
    tags=["EmptyOperator", "Airflow 3.x", "BashOperator", "PythonOperator"]
) as dag:
    # Tworzenie zadań, sensorów
    start = EmptyOperator(
        task_id="start"
    )

    generate_file = BashOperator(
        task_id="generate_file",
        bash_command="echo '5 17 96 35 14' > /home/student/airflow-20260522/data/txt/numbers.txt"
    )

    get_average = PythonOperator(
        task_id="get_average",
        python_callable=get_avg_from
    )

    start >> generate_file >> get_average