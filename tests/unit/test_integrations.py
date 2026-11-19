
import httpx
import pytest

from app.integrations.jellyfin import JellyfinClient
from app.integrations.radarr import RadarrClient


@pytest.mark.asyncio
async def test_radarr_system_status_and_authentication():
    def handler(request):
        assert request.url.path == "/api/v3/system/status"
        assert request.headers["X-Api-Key"] == "test-radarr-key"

        return httpx.Response(
            200,
            json={"version": "5.0.0"},
        )

    client = RadarrClient(
        "http://radarr.test",
        "test-radarr-key",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.get_system_status()
        assert result["version"] == "5.0.0"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_radarr_movie_lookup():
    def handler(request):
        assert request.url.path == "/api/v3/movie/lookup"
        assert request.url.params["term"] == "Dune"

        return httpx.Response(
            200,
            json=[{"title": "Dune", "year": 2021}],
        )

    client = RadarrClient(
        "http://radarr.test",
        "key",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.lookup_movie("Dune")
        assert result[0]["title"] == "Dune"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_radarr_invalid_movie_response():
    client = RadarrClient(
        "http://radarr.test",
        "key",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"invalid": True})
        ),
    )

    try:
        with pytest.raises(ValueError, match="Invalid Radarr movies"):
            await client.get_movies()
    finally:
        await client.close()


def test_radarr_rejects_unsupported_api_version():
    with pytest.raises(ValueError, match="Unsupported Radarr"):
        RadarrClient("http://radarr.test", "key", api_version=2)


@pytest.mark.asyncio
async def test_jellyfin_system_info_and_authentication():
    def handler(request):
        assert request.url.path == "/System/Info"
        assert request.headers["X-Emby-Token"] == "test-jellyfin-key"

        return httpx.Response(
            200,
            json={"ServerName": "FoxMedia Jellyfin"},
        )

    client = JellyfinClient(
        "http://jellyfin.test",
        "test-jellyfin-key",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.get_system_info()
        assert result["ServerName"] == "FoxMedia Jellyfin"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_jellyfin_search_items():
    def handler(request):
        assert request.url.path == "/Items"
        assert request.url.params["searchTerm"] == "Frieren"
        assert request.url.params["Recursive"] == "true"
        assert request.url.params["IncludeItemTypes"] == "Series"
        assert request.url.params["Limit"] == "10"

        return httpx.Response(
            200,
            json={
                "Items": [{"Id": "123", "Name": "Frieren"}],
                "TotalRecordCount": 1,
            },
        )

    client = JellyfinClient(
        "http://jellyfin.test",
        "key",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.search_items(
            "Frieren",
            item_types=("Series",),
            limit=10,
        )
        assert result["TotalRecordCount"] == 1
        assert result["Items"][0]["Name"] == "Frieren"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_jellyfin_get_item():
    def handler(request):
        assert request.url.path == "/Items/item-123"

        return httpx.Response(
            200,
            json={"Id": "item-123", "Name": "Example"},
        )

    client = JellyfinClient(
        "http://jellyfin.test",
        "key",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.get_item("item-123")
        assert result["Id"] == "item-123"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_jellyfin_library_refresh():
    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/Library/Refresh"
        return httpx.Response(204)

    client = JellyfinClient(
        "http://jellyfin.test",
        "key",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.refresh_library()
        assert result == {"accepted": True, "status_code": 204}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_jellyfin_rejects_empty_search():
    client = JellyfinClient(
        "http://jellyfin.test",
        "key",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"Items": []})
        ),
    )

    try:
        with pytest.raises(ValueError, match="cannot be empty"):
            await client.search_items("   ")
    finally:
        await client.close()
