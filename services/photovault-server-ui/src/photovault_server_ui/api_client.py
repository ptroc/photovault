"""Default HTTP client functions for photovault-server-ui."""
from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ApiFetcher = Callable[[str, dict[str, str]], dict[str, Any]]
ApiPoster = Callable[[str, dict[str, Any]], dict[str, Any]]

def _default_api_fetcher(path: str, query: dict[str, str]) -> dict[str, Any]:
    base_url = os.getenv("PHOTOVAULT_SERVER_UI_API_BASE_URL", "http://127.0.0.1:9301")
    query_suffix = f"?{urlencode(query)}" if query else ""
    url = f"{base_url}{path}{query_suffix}"
    req = Request(url=url, method="GET")
    with urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _default_api_poster(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    base_url = os.getenv("PHOTOVAULT_SERVER_UI_API_BASE_URL", "http://127.0.0.1:9301")
    url = f"{base_url}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        method="POST",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


