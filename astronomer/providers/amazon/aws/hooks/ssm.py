from typing import Any

from aiobotocore.client import AioBaseClient
from airflow.exceptions import AirflowException

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync


class SSMHookAsync(AwsBaseHookAsync):
    """
    Temporary docstring
    with more stuff here
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "ssm"
        kwargs["resource_type"] = "ssm"
        super().__init__(*args, **kwargs)

    async def command_invocation_is_successful(
        self, client: AioBaseClient, instance_id: str, command_id: str
    ) -> bool:
        """
        Temporary docstring
        with more stuff here
        """
        response = await client.get_command_invocation(
            CommandId=self.command_id,
            InstanceId=self.instance_id,
            PluginName="aws:runPowerShellScript",
        )

        if response["status"] in ["Success"]:
            return True
        elif response["status"] not in ["Pending", "InProgress", "Delayed"]:
            raise AirflowException("ERROR: SSM Job reached a Failed status.")
        return False
