
import httpx
import pytest

from app.core.errors import (
    PermanentJobError,
    RetryableJobError,
    classify_job_error,
)


def test_retryable_job_error():
    error = RetryableJobError("Temporary failure", "SONARR_TIMEOUT")

    assert classify_job_error(error) == (
        "retryable",
        "SONARR_TIMEOUT",
    )


def test_permanent_job_error():
    error = PermanentJobError("Invalid request", "INVALID_MEDIA")

    assert classify_job_error(error) == (
        "permanent",
        "INVALID_MEDIA",
    )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (408, "retryable"),
        (429, "retryable"),
        (500, "retryable"),
        (503, "retryable"),
        (400, "permanent"),
        (401, "permanent"),
        (404, "permanent"),
    ],
)
def test_http_status_error_classification(status, expected):
    request = httpx.Request("GET", "https://example.test")
    response = httpx.Response(status, request=request)

    error = httpx.HTTPStatusError(
        "HTTP error",
        request=request,
        response=response,
    )

    assert classify_job_error(error) == (
        expected,
        f"HTTP_{status}",
    )


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("Timeout"),
        ConnectionError("Connection failed"),
        httpx.ConnectTimeout("Connect timeout"),
        httpx.ReadTimeout("Read timeout"),
        httpx.ConnectError("Connection error"),
        httpx.TransportError("Transport error"),
    ],
)
def test_connection_errors_are_retryable(error):
    assert classify_job_error(error) == (
        "retryable",
        "CONNECTION_ERROR",
    )


def test_unexpected_error():
    error = ValueError("Unexpected input")

    assert classify_job_error(error) == (
        "unexpected",
        "UNEXPECTED_ERROR",
    )
