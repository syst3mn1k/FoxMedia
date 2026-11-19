
from collections.abc import Awaitable
from typing import Any

from app.core.errors import PermanentJobError
from app.integrations.jellyfin import create_jellyfin_client
from app.integrations.radarr import create_radarr_client
from app.integrations.sonarr import create_sonarr_client


def _required_string(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)

    if not isinstance(value, str) or not value.strip():
        raise PermanentJobError(
            f"Missing or invalid field: {field}",
            error_code="INVALID_PAYLOAD",
        )

    return value.strip()


async def _execute(
    client: Any,
    service: str,
    operation: str,
    call: Awaitable[Any],
) -> dict[str, Any]:
    try:
        result = await call
    finally:
        await client.close()

    return {
        "service": service,
        "operation": operation,
        "data": result,
    }


# =========================
# SONARR
# =========================

async def handle_sonarr_status(
    payload: dict[str, Any],
) -> dict[str, Any]:
    client = create_sonarr_client()

    return await _execute(
        client,
        "sonarr",
        "status",
        client.get_system_status(),
    )


async def handle_sonarr_series_list(
    payload: dict[str, Any],
) -> dict[str, Any]:
    client = create_sonarr_client()

    return await _execute(
        client,
        "sonarr",
        "series.list",
        client.get_series(),
    )


async def handle_sonarr_series_lookup(
    payload: dict[str, Any],
) -> dict[str, Any]:
    query = _required_string(payload, "query")
    client = create_sonarr_client()

    return await _execute(
        client,
        "sonarr",
        "series.lookup",
        client.lookup_series(query),
    )


# =========================
# RADARR
# =========================

async def handle_radarr_status(
    payload: dict[str, Any],
) -> dict[str, Any]:
    client = create_radarr_client()

    return await _execute(
        client,
        "radarr",
        "status",
        client.get_system_status(),
    )


async def handle_radarr_movies_list(
    payload: dict[str, Any],
) -> dict[str, Any]:
    client = create_radarr_client()

    return await _execute(
        client,
        "radarr",
        "movies.list",
        client.get_movies(),
    )


async def handle_radarr_movies_lookup(
    payload: dict[str, Any],
) -> dict[str, Any]:
    query = _required_string(payload, "query")
    client = create_radarr_client()

    return await _execute(
        client,
        "radarr",
        "movies.lookup",
        client.lookup_movie(query),
    )


# =========================
# JELLYFIN
# =========================

async def handle_jellyfin_status(
    payload: dict[str, Any],
) -> dict[str, Any]:
    client = create_jellyfin_client()

    return await _execute(
        client,
        "jellyfin",
        "status",
        client.get_system_info(),
    )


async def handle_jellyfin_items_search(
    payload: dict[str, Any],
) -> dict[str, Any]:
    query = _required_string(payload, "query")

    item_types = payload.get("item_types", ["Movie", "Series"])

    if (
        not isinstance(item_types, list)
        or not all(isinstance(item, str) for item in item_types)
    ):
        raise PermanentJobError(
            "item_types must be a list of strings",
            error_code="INVALID_PAYLOAD",
        )

    limit = payload.get("limit", 20)

    if isinstance(limit, bool) or not isinstance(limit, int):
        raise PermanentJobError(
            "limit must be an integer",
            error_code="INVALID_PAYLOAD",
        )

    client = create_jellyfin_client()

    return await _execute(
        client,
        "jellyfin",
        "items.search",
        client.search_items(
            query,
            item_types=tuple(item_types),
            limit=limit,
        ),
    )


async def handle_jellyfin_item_get(
    payload: dict[str, Any],
) -> dict[str, Any]:
    item_id = _required_string(payload, "item_id")
    client = create_jellyfin_client()

    return await _execute(
        client,
        "jellyfin",
        "items.get",
        client.get_item(item_id),
    )


async def handle_jellyfin_library_refresh(
    payload: dict[str, Any],
) -> dict[str, Any]:
    client = create_jellyfin_client()

    return await _execute(
        client,
        "jellyfin",
        "library.refresh",
        client.refresh_library(),
    )
