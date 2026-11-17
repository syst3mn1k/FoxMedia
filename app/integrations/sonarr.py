from typing import Any

import httpx

from app.core.config import settings


class SonarrClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_version: int = 3,
    ) -> None:
        if api_version not in (3, 5):
            raise ValueError("Unsupported Sonarr API version")

        self.api_version = api_version

        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/",
            headers={"X-Api-Key": api_key},
            timeout=httpx.Timeout(10.0, connect=3.0),
        )

    async def get_system_status(self) -> dict[str, Any]:
        response = await self._client.get(
            f"api/v{self.api_version}/system/status"
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("Invalid Sonarr response")

        return data

    async def get_series(self) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"api/v{self.api_version}/series"
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Invalid Sonarr series response")

        if not all(isinstance(item, dict) for item in data):
            raise ValueError("Invalid Sonarr series entries")

        return data

    async def close(self) -> None:
        await self._client.aclose()


def create_sonarr_client() -> SonarrClient:
    if not settings.sonarr_base_url:
        raise RuntimeError("SONARR_BASE_URL is missing")

    if settings.sonarr_api_key is None:
        raise RuntimeError("SONARR_API_KEY is missing")

    return SonarrClient(
        base_url=settings.sonarr_base_url,
        api_key=settings.sonarr_api_key.get_secret_value(),
        api_version=settings.sonarr_api_version,
    )
