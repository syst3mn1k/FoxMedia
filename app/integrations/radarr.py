
from typing import Any

import httpx

from app.core.config import settings


class RadarrClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_version: int = 3,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if api_version != 3:
            raise ValueError("Unsupported Radarr API version")

        self.api_version = api_version

        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/",
            headers={
                "X-Api-Key": api_key,
                "Accept": "application/json",
            },
            timeout=httpx.Timeout(10.0, connect=3.0),
            transport=transport,
        )

    async def get_system_status(self) -> dict[str, Any]:
        response = await self._client.get(
            f"api/v{self.api_version}/system/status"
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("Invalid Radarr system status response")

        return data

    async def get_movies(self) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"api/v{self.api_version}/movie"
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Invalid Radarr movies response")

        if not all(isinstance(item, dict) for item in data):
            raise ValueError("Invalid Radarr movie entries")

        return data

    async def lookup_movie(
        self,
        query: str,
    ) -> list[dict[str, Any]]:
        query = query.strip()

        if not query:
            raise ValueError("Movie lookup query cannot be empty")

        response = await self._client.get(
            f"api/v{self.api_version}/movie/lookup",
            params={"term": query},
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Invalid Radarr lookup response")

        if not all(isinstance(item, dict) for item in data):
            raise ValueError("Invalid Radarr lookup entries")

        return data

    async def close(self) -> None:
        await self._client.aclose()


def create_radarr_client() -> RadarrClient:
    if not settings.radarr_base_url:
        raise RuntimeError("RADARR_BASE_URL is missing")

    if settings.radarr_api_key is None:
        raise RuntimeError("RADARR_API_KEY is missing")

    return RadarrClient(
        base_url=settings.radarr_base_url,
        api_key=settings.radarr_api_key.get_secret_value(),
        api_version=settings.radarr_api_version,
    )
