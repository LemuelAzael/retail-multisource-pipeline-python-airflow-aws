from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Configuración base del DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 15),
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL secuencial usando scripts Python",
    schedule_interval=None,  # Manual, puedes cambiarlo a cron si quieres automatizar
    catchup=False,
) as dag:

    # Ingesta de datos
    ingest_task = BashOperator(
        task_id="ingest_data",
        bash_command="python /opt/airflow/scripts/ingest.py"
    )

    # Transformación de datos
    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/scripts/transform.py"
    )

    # Carga y disponibilización en Athena
    load_task = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/scripts/load.py"
    )

    # Definir orden de ejecución
    ingest_task >> transform_task >> load_task

