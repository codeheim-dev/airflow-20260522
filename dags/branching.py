from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
import pendulum

def choose_path() -> list:
    from random import randint
    x = randint(0, 9)

    if 1 <= x <= 3:
        return ["task_A"]
    elif 4 <= x <= 6:
        return ["task_B"]
    elif 7 <= x <= 9:
        return ["task_C"]
    else:
        return ["task_A", "task_B", "task_C"]


# Tworzenie DAG-a
with DAG(
    dag_id="branching",
    description="Rozgałęzienie zadań",
    start_date=pendulum.datetime(2026, 5, 22, 8, 0),
    end_date=pendulum.datetime(2026, 5, 31, 0, 0),
    schedule=None,
    catchup=False,
    tags=["Airflow 3.x", "BashOperator", "BranchPythonOperator"]
) as dag:
    
    # Task_IDS = [task_A, task_B, task_C]
    bash_tasks = [
        BashOperator(task_id=f"task_{c}", bash_command="date") for c in "ABC"
    ]

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=choose_path
    )

    branch_task >> bash_tasks
