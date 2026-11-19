
import pytest

import app.workers.dispatcher as dispatcher_module
from app.core.errors import PermanentJobError


@pytest.mark.parametrize(
    ("command_type", "handler_name"),
    [
        ("system.echo", "handle_system_echo"),
        ("sonarr.status", "handle_sonarr_status"),
        ("sonarr.series.list", "handle_sonarr_series_list"),
        ("sonarr.series.lookup", "handle_sonarr_series_lookup"),
        ("radarr.status", "handle_radarr_status"),
        ("radarr.movies.list", "handle_radarr_movies_list"),
        ("radarr.movies.lookup", "handle_radarr_movies_lookup"),
        ("jellyfin.status", "handle_jellyfin_status"),
        ("jellyfin.items.search", "handle_jellyfin_items_search"),
        ("jellyfin.items.get", "handle_jellyfin_item_get"),
        ("jellyfin.library.refresh", "handle_jellyfin_library_refresh"),
    ],
)
@pytest.mark.asyncio
async def test_dispatch_routes_to_correct_handler(
    command_type,
    handler_name,
    monkeypatch,
):
    expected = {"dispatched": command_type}

    async def fake_handler(payload):
        return expected

    monkeypatch.setattr(
        dispatcher_module,
        handler_name,
        fake_handler,
    )

    result = await dispatcher_module.dispatch(
        {"command_type": command_type}
    )

    assert result == expected


@pytest.mark.asyncio
async def test_dispatch_rejects_unknown_command():
    with pytest.raises(PermanentJobError) as exc:
        await dispatcher_module.dispatch(
            {"command_type": "unknown.command"}
        )

    assert exc.value.error_code == "UNKNOWN_COMMAND"
