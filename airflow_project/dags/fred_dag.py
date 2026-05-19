from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "asmae",
    "depends_on_past": False,
}

with DAG(
    dag_id="fred_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 26),
    schedule="@daily",
    catchup=False,
    tags=["crypto", "fred", "kafka", "batch"],
) as dag:

    run_fred_producer = BashOperator(
        task_id="run_fred_producer",
        bash_command="python /opt/airflow/scripts/fred_producer.py"
    )

    run_fred_producer
