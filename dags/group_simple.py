from airflow.sdk import dag, task, task_group

@dag()
def group_dag():

    @task(default_args={"retries": 2})
    def task_1():
        print("Task 1")

    @task_group()
    def my_group():
        @task()
        def task_2():
            print("Task 2")

        @task()
        def task_3():
            print("Task 3")

        task_2() >> task_3()

    task_1() >> my_group()

group_dag()    