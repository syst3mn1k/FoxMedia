from typing import Any

from app.workers.handlers.integrations import (
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
)


async def handle_system_echo(payload: dict[str, Any]) -> dict[str, Any]:
    return {"echo": payload}


__all__ = [
    "handle_system_echo",
    "handle_sonarr_status",
    "handle_sonarr_series_list",
    "handle_sonarr_series_lookup",
    "handle_radarr_status",
    "handle_radarr_movies_list",
    "handle_radarr_movies_lookup",
    "handle_jellyfin_status",
    "handle_jellyfin_items_search",
    "handle_jellyfin_item_get",
    "handle_jellyfin_library_refresh",
]
