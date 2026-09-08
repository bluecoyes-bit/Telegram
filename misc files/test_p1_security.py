"""Tests for the web console API security gate (Phase 18).

Covers mandatory token authentication on /console routes, the secure-by-default
503 when no token is configured, rate limiting, and the background task tracking
(Phase 22).

NOTE: web_console.py previously failed to import (missing `asynccontextmanager`),
which would have crashed the entire application on startup. These tests also
serve as a regression guard that the module imports cleanly.
"""

import pytest

from fastapi.testclient import TestClient
from fastapi import FastAPI

import config
import web_console as w

app = FastAPI()
app.include_router(w.console_router, prefix="/console")
client = TestClient(app)

TOKEN = "test-secret-token"


@pytest.fixture(autouse=True)
def _set_token():
    config.CONFIG["WEB_API_TOKEN"] = TOKEN
    yield
    config.CONFIG["WEB_API_TOKEN"] = ""


def test_module_imports_and_router_gated():
    assert len(w.console_router.dependencies) == 1


def test_no_token_returns_401():
    r = client.get("/console/api/console/accounts")
    assert r.status_code == 401


def test_wrong_token_returns_401():
    r = client.get("/console/api/console/accounts", headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


def test_bearer_token_allowed():
    r = client.get("/console/api/console/accounts", headers={"Authorization": f"Bearer {TOKEN}"})
    assert r.status_code == 200


def test_x_api_token_header_allowed():
    r = client.get("/console/api/console/accounts", headers={"X-API-Token": TOKEN})
    assert r.status_code == 200


def test_secure_by_default_when_token_unset():
    config.CONFIG["WEB_API_TOKEN"] = ""
    # Even with a token supplied, an unconfigured token must refuse (secure default).
    r = client.get("/console/api/console/accounts", headers={"Authorization": f"Bearer {TOKEN}"})
    assert r.status_code == 503


def test_post_destructive_route_requires_token():
    r = client.post("/console/api/console/mass-execute", json={"target_channel": "@x"})
    assert r.status_code == 401


def test_rate_limit_applied():
    w._rate_buckets.clear()
    # Exceed the per-IP window limit and observe 429 for an authenticated client.
    for _ in range(w._RATE_LIMIT_MAX + 2):
        client.get("/console/api/console/accounts", headers={"Authorization": f"Bearer {TOKEN}"})
    r = client.get("/console/api/console/accounts", headers={"Authorization": f"Bearer {TOKEN}"})
    assert r.status_code == 429
    w._rate_buckets.clear()
