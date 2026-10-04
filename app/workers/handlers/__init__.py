from typing import Any


async def handle_system_echo(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "echo": payload,
    }
