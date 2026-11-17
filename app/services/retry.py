import random


def calculate_backoff(
    attempt: int,
    base_delay: float = 5.0,
    max_delay: float = 300.0,
) -> float:
    if attempt < 0:
        raise ValueError("Attempt cannot be negative")

    if base_delay <= 0 or max_delay <= 0:
        raise ValueError("Backoff delays must be positive")

    delay = min(max_delay, base_delay * (2 ** attempt))

    return delay + random.uniform(0, 1.0)
