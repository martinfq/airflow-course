from typing import Callable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def sql_task(python_callable: Callable | None = None, **operator_kwargs):

    def decorator(func: Callable):

        def wrapper(*args, **kwargs) -> SQLExecuteQueryOperator:
            sql = func(*args, **kwargs)

            if not isinstance(sql, str) or not sql.strip():
                raise ValueError(
                    "@sql_task function must return a non-empty SQL string"
                )

            # 🔥 Extraer task_id correctamente
            task_id = operator_kwargs.get("task_id", func.__name__)

            # 🔥 Evitar duplicación
            clean_kwargs = {k: v for k, v in operator_kwargs.items() if k != "task_id"}

            return SQLExecuteQueryOperator(
                task_id=task_id,
                sql=sql,
                **clean_kwargs,
            )

        return wrapper

    if python_callable:
        return decorator(python_callable)

    return decorator