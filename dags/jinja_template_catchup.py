from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import random

def create_csv(plik: str, ilosc: int = 5) -> None:
    import csv
    """Generuje losowe liczby i zapisuje je do pliku CSV."""
    with open(plik, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "wartosc"])
        for i in range(1, ilosc + 1):
            writer.writerow([i, round(random.uniform(0, 100), 2)])
    print(f"Zapisano {ilosc} wierszy do '{plik}'.")


# Tworzenie DAG-a
with DAG(
    dag_id="jinja_template_catchup",
    description="Jinja Template",
    start_date=pendulum.datetime(2026, 5, 1, 8, 0),
    end_date=pendulum.datetime(2026, 5, 31, 8, 0),
    schedule="0 8 * * *",
    catchup=True,
    tags=["Airflow 3.x", "Jinja Template", "PythonOperator"]
) as dag:

    generate_file = PythonOperator(
        task_id="generate_file",
        python_callable=create_csv,
        op_kwargs={
            "plik": "/home/student/airflow-20260522/data/csv/records_{{ ds_nodash }}.csv",
            "ilosc": 10
        }
    )

    generate_file