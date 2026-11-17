from typing import Any

from app.core.errors import PermanentJobError
from app.workers.handlers import handle_system_echo


async def dispatch(payload: dict[str, Any]) -> dict[str, Any]:
    command_type = payload.get("command_type")

    if command_type == "system.echo":
        return await handle_system_echo(payload)

    raise PermanentJobError(
        f"Unknown command_type: {command_type}",
        error_code="UNKNOWN_COMMAND",
    )
