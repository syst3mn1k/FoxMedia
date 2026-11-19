
from typing import Any
from urllib.parse import quote

import httpx

from app.core.config import settings


class JellyfinClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/",
            headers={
                "X-Emby-Token": api_key,
                "Accept": "application/json",
            },
            timeout=httpx.Timeout(15.0, connect=3.0),
            transport=transport,
        )

    async def get_system_info(self) -> dict[str, Any]:
        response = await self._client.get("System/Info")
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("Invalid Jellyfin system info response")

        return data

    async def search_items(
        self,
        search_term: str,
        item_types: tuple[str, ...] = ("Movie", "Series"),
        limit: int = 20,
    ) -> dict[str, Any]:
        search_term = search_term.strip()

        if not search_term:
            raise ValueError("Jellyfin search term cannot be empty")

        if not item_types or not all(
            isinstance(item, str) and item.strip()
            for item in item_types
        ):
            raise ValueError("Invalid Jellyfin item types")

        if not 1 <= limit <= 100:
            raise ValueError("Jellyfin search limit must be between 1 and 100")

        response = await self._client.get(
            "Items",
            params={
                "searchTerm": search_term,
                "Recursive": "true",
                "IncludeItemTypes": ",".join(item_types),
                "Limit": limit,
            },
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("Invalid Jellyfin search response")

        items = data.get("Items", [])

        if not isinstance(items, list):
            raise ValueError("Invalid Jellyfin Items response")

        if not all(isinstance(item, dict) for item in items):
            raise ValueError("Invalid Jellyfin item entries")

        return data

    async def get_item(
        self,
        item_id: str,
    ) -> dict[str, Any]:
        item_id = item_id.strip()

        if not item_id:
            raise ValueError("Jellyfin item ID cannot be empty")

        safe_item_id = quote(item_id, safe="")

        response = await self._client.get(
            f"Items/{safe_item_id}"
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("Invalid Jellyfin item response")

        return data

    async def refresh_library(self) -> dict[str, Any]:
        response = await self._client.post("Library/Refresh")
        response.raise_for_status()

        return {
            "accepted": True,
            "status_code": response.status_code,
        }

    async def close(self) -> None:
        await self._client.aclose()


def create_jellyfin_client() -> JellyfinClient:
    if not settings.jellyfin_base_url:
        raise RuntimeError("JELLYFIN_BASE_URL is missing")

    if settings.jellyfin_api_key is None:
        raise RuntimeError("JELLYFIN_API_KEY is missing")

    return JellyfinClient(
        base_url=settings.jellyfin_base_url,
        api_key=settings.jellyfin_api_key.get_secret_value(),
    )
