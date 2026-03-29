import csv
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
import requests    
from airflow.providers.postgres.hooks.postgres import PostgresHook



@dag(
    dag_id="user_processing_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


    @task.sensor(poke_interval=10, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get("http://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"API response status: {response.status_code}")
        if response.status_code == 200:
            condition = True
            fake_users = response.json()
        else:
            condition = False
            fake_users = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_users)
    
    @task
    def _extract_user(fake_users):
        return {
            "id": fake_users['id'],
            "firstname": fake_users['personalInfo']['firstName'],
            "lastname": fake_users['personalInfo']['lastName'],
            "email": fake_users['personalInfo']['email']
        }

    @task
    def process_user(user_info):
        user_info['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open('/tmp/user_info.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users (id, firstname, lastname, email, created_at) FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )
    process_user(_extract_user( create_table >> is_api_available())) >> store_user()

dag = user_processing()
