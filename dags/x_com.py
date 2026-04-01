from airflow.sdk import dag, task, Context
from typing import Dict, Any
@dag()
def x_com_dag():

    @task()
    def task_1() -> int:
        print("Task 1")
        my_val = 67
        my_sentence = "Hello World"
        # return val #Es lo mismo que context.push
        return {"my_val": my_val, "my_sentence": my_sentence}
    
    @task()
    def task_2(data: Dict[str, Any]):
        print("Task 2")
        print(f"Value from Task 1: {data['my_val']}")

    @task()
    def task_3():
        print("Task 3")

    val = task_1() 
    task_2(val) >> task_3() 

x_com_dag()


#Ejemplo simple:
# @task()
#     def task_1(context: Context):
#         print("Task 1")
#         val = 67
#         context.xcom_push(key="task_1_val", value=val)

    
#     @task()
#     def task_2(context: Context):
#         print("Task 2")
#         val = context.xcom_pull(key="task_1_val", task_ids="task_1")
#         print(f"Value from Task 1: {val}")