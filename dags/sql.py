from datetime import datetime
from airflow import DAG

# importa tu decorator
from my_sdk.decorators.sql import sql_task


# Definición del DAG
with DAG(
    dag_id="example_sql_count_xcom",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    @sql_task(
        task_id="count_xcoms",
        conn_id="postgres",  # conexión a la DB de Airflow
    )
    def count_xcoms():
        return "SELECT COUNT(*) FROM xcom"


    # Crear la tarea
    count_task = count_xcoms()

    #Host -> postgres , user y pass en docker compose