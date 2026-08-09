# =============================================================================
# APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
# Bay of Plenty Corridor · Independent Research Project
# Script: api_retry.py
# Stage:  Integration (shared utility)
# Author: Gabriela Olivera | Data Analytics Portfolio
# Version: 4.1.0 | 2026-05
# =============================================================================

"""
Wraps requests.get() with bounded exponential-backoff retries for
transient failures only: connection errors, timeouts, HTTP 429, and any
HTTP 5xx. A 4xx response other than 429 (bad request, not found, etc.)
is never retried — raise_for_status() is called immediately so that
kind of failure surfaces right away instead of being masked behind a
retry loop that can't fix a client-side mistake.

Used by api_feed.py's fetch_live_apis() (Open-Meteo, Frankfurter,
Overpass). Ported from the sister project
(horticultural-land-suitability-nz / Terroir)'s src/api_retry.py — same
bounded-retry design, kept consistent rather than reinventing it. Kept
dependency-light: stdlib `time` plus the `requests` library.
"""

import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def get_with_retry(
    url: str,
    params: Optional[dict] = None,
    timeout: float = 5,
    max_retries: int = 4,
    backoff_base_seconds: float = 1.0,
    method: str = "GET",
    data: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> requests.Response:
    """
    GET by default; pass method="POST" (with `data`/`headers`) for
    Overpass-style requests. Same bounded-retry/backoff logic either way.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            response = requests.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout,
            )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            if attempt > max_retries:
                raise
            _wait_before_retry(attempt, backoff_base_seconds, reason=type(exc).__name__)
            continue

        is_retryable = response.status_code in RETRYABLE_STATUS_CODES
        if is_retryable and attempt <= max_retries:
            _wait_before_retry(
                attempt,
                backoff_base_seconds,
                reason=f"HTTP {response.status_code}",
                retry_after=response.headers.get("Retry-After"),
            )
            continue

        # Raises immediately for any other 4xx (or the final retryable
        # failure once max_retries is exhausted) — never retried further.
        response.raise_for_status()
        return response


def _wait_before_retry(
    attempt: int,
    backoff_base_seconds: float,
    reason: str,
    retry_after: Optional[str] = None,
) -> None:
    if retry_after is not None:
        try:
            delay = float(retry_after)
        except ValueError:
            delay = backoff_base_seconds * (2 ** (attempt - 1))
    else:
        delay = backoff_base_seconds * (2 ** (attempt - 1))

    logger.warning(f"{reason} — retrying in {delay:.1f}s (attempt {attempt})...")
    time.sleep(delay)
