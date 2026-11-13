"""Shared HTTP session with retries and back-off for the data.gov.my API."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "transit-pulse/0.1 (data engineering portfolio project)"


def make_session(total_retries: int = 5) -> requests.Session:
    retry = Retry(
        total=total_retries,
        backoff_factor=1.0,  # 1s, 2s, 4s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = USER_AGENT
    return session
