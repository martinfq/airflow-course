from airflow.sdk import dag, task

@dag()
def branch_dag():

    @task()
    def task_1():
        return 45
    
    @task.branch
    def task_2(val : int):
        print("Task 2")
        if val== 1:
            return ["equal_1", "similar_1"]
        return "not_1"
    
    @task()
    def equal_1(val : int):
        print("Task 3")
        print(f"Value from Task 1: {val}")

    @task()
    def similar_1():
        print("Task 5")
    
    @task()
    def not_1(val : int):
        print("Task 4")
        print(f"Value from Task 1: {val}")

    val = task_1()
    task_2(val) >> [equal_1(val), not_1(val)]
    
branch_dag()