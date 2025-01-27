from unittest import mock

import pendulum
from airflow.models import DAG, Connection
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone


def get_dag_run(dag_id: str = "test_dag_id", run_id: str = "test_dag_id") -> DagRun:
    dag_run = DagRun(
        dag_id=dag_id, run_type="manual", execution_date=timezone.datetime(2022, 1, 1), run_id=run_id
    )
    return dag_run


def get_task_instance(task: BaseOperator) -> TaskInstance:
    return TaskInstance(task, timezone.datetime(2022, 1, 1))


def get_conn() -> Connection:
    return Connection(
        conn_id="test_conn",
        extra={},
    )


def create_context(task):
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("Europe/Amsterdam")
    execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date)
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "ts": execution_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": dag_run.run_id,
    }
