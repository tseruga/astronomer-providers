from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)
from astronomer.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSensorTrigger,
)

TASK_ID = "redshift_sensor_check"
POLLING_PERIOD_SECONDS = 1.0


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_redshift_cluster_sensor_async():
    """Test RedshiftClusterSensorAsync that a task with wildcard=True
    is deferred and an RedshiftClusterSensorTrigger will be fired when executed method is called"""
    task = RedshiftClusterSensorAsync(
        task_id=TASK_ID,
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(
        exc.value.trigger, RedshiftClusterSensorTrigger
    ), "Trigger is not a RedshiftClusterSensorTrigger"


def test_redshift_sensor_async_execute_failure(context):
    """Test RedshiftClusterSensorAsync with an AirflowException is raised in case of error event"""
    task = RedshiftClusterSensorAsync(
        task_id=TASK_ID,
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_redshift_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = RedshiftClusterSensorAsync(
        task_id=TASK_ID,
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event={"status": "success", "cluster_state": "available"})
    mock_log_info.assert_called_with(
        "Cluster Identifier %s is in %s state", "astro-redshift-cluster-1", "available"
    )


def test_redshift_sensor_async_execute_complete_event_none():
    """Asserts that logging occurs as expected"""
    task = RedshiftClusterSensorAsync(
        task_id=TASK_ID,
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event=None)
    mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)


def test_poll_interval_deprecation_warning():
    """Test DeprecationWarning for RedshiftClusterSensorAsync by setting param poll_interval"""
    # TODO: Remove once deprecated
    with pytest.warns(expected_warning=DeprecationWarning):
        RedshiftClusterSensorAsync(
            task_id=TASK_ID,
            cluster_identifier="astro-redshift-cluster-1",
            target_status="available",
            poll_interval=5.0,
        )
