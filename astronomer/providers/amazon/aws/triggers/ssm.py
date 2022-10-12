import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.ssm import SSMHookAsync


class SSMJobTrigger(BaseTrigger):
    """
    Temporary docstring
    with more stuff here
    """

    def __init__(
        self,
        instance_id: str,
        command_id: str,
        description: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        poke_interval: float = 5,
        **hook_params: Any,
    ):

        super().__init__()
        self.instance_id = instance_id
        self.command_id = command_id
        self.description = description
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize SSMJobTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.s3.SSMJobTrigger",
            {
                "instance_id": self.instance_id,
                "command_id": self.command_id,
                "description": self.description,
                "aws_conn_id": self.aws_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make an asynchronous connection using SSMHookAsync."""
        try:
            hook = self._get_async_hook()
            async with await hook.get_client_async() as client:
                while True:
                    if await hook.command_invocation_is_successful(client, self.instance_id, self.command_id):
                        yield TriggerEvent({"status": "success"})
                    else:
                        await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> SSMHookAsync:
        return SSMHookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))
