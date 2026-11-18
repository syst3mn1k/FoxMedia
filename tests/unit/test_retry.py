
import pytest

from app.services.retry import calculate_backoff


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [
        (0, 5.25),
        (1, 10.25),
        (2, 20.25),
    ],
)
def test_calculate_backoff_exponential(attempt, expected, monkeypatch):
    monkeypatch.setattr(
        "app.services.retry.random.uniform",
        lambda low, high: 0.25,
    )

    assert calculate_backoff(attempt) == expected


def test_calculate_backoff_respects_max_delay(monkeypatch):
    monkeypatch.setattr(
        "app.services.retry.random.uniform",
        lambda low, high: 0.25,
    )

    assert calculate_backoff(10, base_delay=5, max_delay=30) == 30.25


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"attempt": -1}, "Attempt cannot be negative"),
        ({"attempt": 1, "base_delay": 0}, "Backoff delays must be positive"),
        ({"attempt": 1, "max_delay": 0}, "Backoff delays must be positive"),
        ({"attempt": 1, "base_delay": -5}, "Backoff delays must be positive"),
    ],
)
def test_calculate_backoff_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        calculate_backoff(**kwargs)
