from datetime import timedelta
from typing import Any, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from astronomer.providers.amazon.triggers.ssm import SSMJobTrigger
from astronomer.providers.utils.typing_compat import Context


class SSMJobSensorAsync(BaseSensorOperator):
    """
    Temporary docstring
    with more stuff here
    """

    template_fields: Sequence[str] = ("instance_id", "command_id")

    def __init__(
        self,
        *,
        instance_id: str,
        command_id: str,
        description: Optional[str] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.command_id = command_id
        self.description = description

        # TODO: How did sftp do this?
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """
        Temporary docstring
        with more stuff here
        """
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=SSMJobTrigger(
                instance_id=self.instance_id,
                command_id=self.command_id,
                description=self.description,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any) -> None:
        """
        Temporary docstring
        with more stuff here
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        elif event["status"] == "success":
            return None
        return None
