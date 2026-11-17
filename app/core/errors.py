import httpx


class RetryableJobError(Exception):
    """Temporary error that may succeed on retry."""

    def __init__(
        self,
        message: str,
        error_code: str = "TRANSIENT_ERROR",
    ) -> None:
        super().__init__(message)
        self.error_code = error_code


class PermanentJobError(Exception):
    """Error that must not be retried."""

    def __init__(
        self,
        message: str,
        error_code: str = "PERMANENT_ERROR",
    ) -> None:
        super().__init__(message)
        self.error_code = error_code


def classify_job_error(
    error: Exception,
) -> tuple[str, str]:
    if isinstance(error, RetryableJobError):
        return "retryable", error.error_code

    if isinstance(error, PermanentJobError):
        return "permanent", error.error_code

    if isinstance(error, httpx.HTTPStatusError):
        code = error.response.status_code

        if code in (408, 429) or code >= 500:
            return "retryable", f"HTTP_{code}"

        return "permanent", f"HTTP_{code}"

    if isinstance(
        error,
        (
            TimeoutError,
            ConnectionError,
            httpx.TimeoutException,
            httpx.TransportError,
        ),
    ):
        return "retryable", "CONNECTION_ERROR"

    return "unexpected", "UNEXPECTED_ERROR"
