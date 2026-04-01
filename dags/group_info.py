from airflow.sdk import dag, task, task_group

@dag()
def group_info():

    @task(default_args={"retries": 2})
    def a():
        print("Task 1")
        return 56

    @task_group()
    def my_group(val : int):
        @task()
        def b(my_val : int):
            print("Task 2")
            print(f"Value from Task 1: {my_val + 2}")

        @task()
        def c():
            print("Task 3")

        b(val) >> c()

    val = a() 
    my_group(val)

group_info()    