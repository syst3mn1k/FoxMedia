
from typing import Any

from app.core.errors import PermanentJobError
from app.workers.handlers import (
    handle_jellyfin_item_get,
    handle_jellyfin_items_search,
    handle_jellyfin_library_refresh,
    handle_jellyfin_status,
    handle_radarr_movies_list,
    handle_radarr_movies_lookup,
    handle_radarr_status,
    handle_sonarr_series_list,
    handle_sonarr_series_lookup,
    handle_sonarr_status,
    handle_system_echo,
)


async def dispatch(payload: dict[str, Any]) -> dict[str, Any]:
    command_type = payload.get("command_type")

    handlers = {
        "system.echo": handle_system_echo,

        "sonarr.status": handle_sonarr_status,
        "sonarr.series.list": handle_sonarr_series_list,
        "sonarr.series.lookup": handle_sonarr_series_lookup,

        "radarr.status": handle_radarr_status,
        "radarr.movies.list": handle_radarr_movies_list,
        "radarr.movies.lookup": handle_radarr_movies_lookup,

        "jellyfin.status": handle_jellyfin_status,
        "jellyfin.items.search": handle_jellyfin_items_search,
        "jellyfin.items.get": handle_jellyfin_item_get,
        "jellyfin.library.refresh": handle_jellyfin_library_refresh,
    }

    handler = handlers.get(command_type)

    if handler is None:
        raise PermanentJobError(
            f"Unknown command_type: {command_type}",
            error_code="UNKNOWN_COMMAND",
        )

    return await handler(payload)
