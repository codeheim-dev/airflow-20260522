from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

def get_random_number(start: int = 0, end: int = 100) -> None:
    from random import randint
    print(f"Losuje wartość z zakresu {start} - {end}")
    print(f"Wartość: {randint(start, end)}")

# Tworzenie DAG-a
with DAG(
    dag_id="bash_python_operator",
    description="Pokazanie operatorów Bash i Python",
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

    bash_operator = BashOperator(
        task_id="bash_operator",
        bash_command="ls -l /home/student/airflow-20260522"
    )

    no_args_operator = PythonOperator(
        task_id="no_args_operator",
        python_callable=get_random_number
    )

    args_operator = PythonOperator(
        task_id="args_operator",
        python_callable=get_random_number,
        op_args=[80, 90]  # Kolejność jak w def ... czyli [0] - wartość I argumentu
    )

    kwargs_operator = PythonOperator(
        task_id="kwargs_operator",
        python_callable=get_random_number,
        op_kwargs={
            "start": 10,
            "end": 30
        }
    )

    # Flow (kolejność wykonywania zadań)
    python_tasks = [no_args_operator, args_operator, kwargs_operator]
    start >> bash_operator >> python_tasks
