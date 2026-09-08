#!/usr/bin/env python3
"""
resource_manager.py
Consolidated resource orchestration layer for the Telegram Suite.
Manages proxies, sessions, and account leases with strict async/thread boundaries.

USAGE:
  Replace old imports in your codebase with:
    from resource_manager import ProxyManager, ProxyLeaseManager
    from resource_manager import AccountLeaseManager, AccountState
    from resource_manager import SessionManager, SessionAlreadyOwnedError, SessionLifecycleState
"""
from __future__ import annotations

import asyncio
import enum
import hashlib
import logging
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from exception_classifier import ErrorCategory, classify_exception

logger = logging.getLogger("ResourceManager")

# ────────────────────────────────────────────────────────────────
# 0. SHARED UTILITIES & UNIFIED CONSTANTS
# ────────────────────────────────────────────────────────────────
def compute_session_fingerprint(session_str: str, api_id: int) -> str:
    """Deterministic fingerprint from session string + api_id."""
    raw = f"{session_str}:{api_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

# Unified terminal/eligible sets to prevent state drift across modules
TERMINAL_DB_STATUSES = frozenset({
    "revoked", "banned", "deactivated", "invalid",
    "auth_key_duplicated", "permanently_failed", "quarantined",
})
ELIGIBLE_DB_STATUSES = frozenset({
    "active", "pending", "2fa_required", "restricted",
})
TERMINAL_STATUSES = TERMINAL_DB_STATUSES  # Backward compatibility alias

# ────────────────────────────────────────────────────────────────
# 1. PROXY LAYER (Sync Testing + Async Lease Management)
# ────────────────────────────────────────────────────────────────
PROXY_TEST_URL: str = "https://httpbin.org/ip"
PROXY_TEST_TIMEOUT: float = 5.0
PROXY_TEST_TIMEOUT_SLOW: float = 15.0
MAX_PROXY_FAILURES: int = 3
PROXY_ROTATION_WINDOW: float = 5 * 60
MIN_WORKING_PROXIES_TO_START: int = 1
TEST_BATCH_SIZE: int = 50
MAX_WORKERS_DEFAULT: int = 30
CONNECTION_POOL_SIZE: int = 20
PROXY_COOLDOWN_SECONDS: int = 600  # legacy default, superseded by the humanized window below
PROXY_COOLDOWN_MIN_SECONDS: float = 180.0   # human rest window after every disconnect: 3-5 min
PROXY_COOLDOWN_MAX_SECONDS: float = 300.0
PROXY_MICRO_JITTER_SECONDS: Tuple[float, float] = (2.0, 8.0)  # micro delay on top of the rest window
ACCOUNT_COOLDOWN_SECONDS: int = 900
COOLDOWN_CHECK_INTERVAL: float = 5.0
PROXY_LEASE_STALE_TTL: float = 900.0  # orphaned leases (no live session) reaped after this
PROXY_PROVIDER: str = os.environ.get("PROXY_PROVIDER", "file").lower()
PROXY_ACQUIRE_TIMEOUT: float = 330.0  # wait up to one full cooldown window for a free proxy
# Keep N proxies free for NEW LOGINS: normal operations (campaigns, auditor)
# may never lease into this reserve, so a login is always possible.
LOGIN_RESERVED_PROXIES: int = int(os.environ.get("LOGIN_RESERVED_PROXIES", "2"))


def humanized_proxy_cooldown() -> float:
    """3-5 minute proxy rest window plus a few seconds of micro-jitter."""
    return random.uniform(PROXY_COOLDOWN_MIN_SECONDS, PROXY_COOLDOWN_MAX_SECONDS) + random.uniform(*PROXY_MICRO_JITTER_SECONDS)

try:
    from colorama import Fore, Style
    G, Y, R, RS = Fore.GREEN, Fore.YELLOW, Fore.RED, Style.RESET_ALL
except ImportError:
    G = Y = R = RS = ""

@dataclass
class ProxyNode:
    proxy_id: str
    host: str
    port: int
    protocol: str = "socks5"
    username: Optional[str] = None
    password: Optional[str] = None
    url: str = ""
    latency: float = 5000.0
    health_state: str = "unknown"
    failure_count: int = 0
    is_leased: bool = False
    leased_to: Optional[str] = None
    lease_id: Optional[str] = None
    lease_time: float = 0.0
    cooldown_until: float = 0.0
    last_tested: float = 0.0
    provider: str = "file"
    added_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not self.url:
            self.url = self._build_url()

    def _build_url(self) -> str:
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"

    def to_telethon_proxy(self) -> Optional[Dict[str, Any]]:
        if not self.host or not self.port:
            return None
        return {"proxy_type": self.protocol, "addr": self.host, "port": self.port,
                "rdns": True, "username": self.username, "password": self.password}

    def to_telethon_tuple(self) -> Optional[Tuple[str, str, int, bool, Optional[str], Optional[str]]]:
        if not self.host or not self.port:
            return None
        return (self.protocol, self.host, self.port, True, self.username, self.password)

    def is_in_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    def acquire(self, phone: str, lease_id: str) -> bool:
        if self.is_leased or self.is_in_cooldown():
            return False
        self.is_leased = True
        self.leased_to = phone
        self.lease_id = lease_id
        self.lease_time = time.time()
        return True

    def release(self) -> None:
        self.is_leased = False
        self.leased_to = None
        self.lease_id = None
        self.lease_time = 0.0

    def put_in_cooldown(self, duration_seconds: Optional[float] = None) -> None:
        self.release()
        self.cooldown_until = time.time() + (duration_seconds if duration_seconds is not None else humanized_proxy_cooldown())
        self.failure_count += 1

    def start_cooldown(self, duration_seconds: Optional[float] = None) -> None:
        """Rest the proxy without counting it as a failure (normal human pacing)."""
        self.cooldown_until = time.time() + (duration_seconds if duration_seconds is not None else humanized_proxy_cooldown())

    def safe_label(self) -> str:
        return f"{self.host}:{self.port}"

class ProxyProvider:
    name: str = "base"
    def load(self) -> List[ProxyNode]:
        raise NotImplementedError

class FileProxyProvider(ProxyProvider):
    name = "file"
    def __init__(self, file_path: str = "proxies.txt"):
        self.file_path = file_path
    def load(self) -> List[ProxyNode]:
        nodes: List[ProxyNode] = []
        if not os.path.exists(self.file_path):
            logger.warning(f"Proxy file not found: {self.file_path}")
            return nodes
        with open(self.file_path, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                node = self._parse_line(line, idx)
                if node:
                    nodes.append(node)
        logger.info(f"FileProxyProvider loaded {len(nodes)} proxies from {self.file_path}")
        return nodes

    def _parse_line(self, line: str, idx: int) -> Optional[ProxyNode]:
        protocol = "socks5"
        username: Optional[str] = None
        password: Optional[str] = None
        if "://" in line:
            proto_part, rest = line.split("://", 1)
            if proto_part.lower() in ("http", "https", "socks4", "socks5"):
                protocol = proto_part.lower()
                line = rest
        parts = line.split(":")
        if len(parts) < 2:
            return None
        try:
            host = parts[0]
            port = int(parts[1])
            if len(parts) >= 4:
                username = parts[2] or None
                password = parts[3] or None
        except (ValueError, IndexError):
            return None
        return ProxyNode(proxy_id=f"file_{idx}_{host}_{port}", host=host, port=port,
                         protocol=protocol, username=username, password=password, provider=self.name)

class DecodoProxyProvider(ProxyProvider):
    name = "decodo"
    def __init__(self):
        self.username = os.environ.get("DECODO_USERNAME", "")
        self.password = os.environ.get("DECODO_PASSWORD", "")
        self.endpoint = os.environ.get("DECODO_HOST", "dc.decodo.com")
        self.port = int(os.environ.get("DECODO_PORT", "10001"))
        self.proxy_count = int(os.environ.get("DECODO_PROXY_COUNT", "10"))
    def load(self) -> List[ProxyNode]:
        if not self.username or not self.password:
            logger.warning("DecodoProxyProvider: DECODO_USERNAME/DECODO_PASSWORD not set")
            return []
        from urllib.parse import quote
        enc_pass = quote(self.password, safe="")
        enc_user = quote(self.username, safe="")
        nodes = []
        for _ in range(self.proxy_count):
            sid = random.randint(100000, 999999)
            nodes.append(ProxyNode(proxy_id=f"decodo_{sid}", host=self.endpoint, port=self.port,
                                   protocol="http", username=self.username, password=self.password,
                                   url=f"http://{enc_user}:{enc_pass}@{self.endpoint}:{self.port}", provider=self.name))
        logger.info(f"DecodoProxyProvider loaded {len(nodes)} rotating proxies")
        return nodes

class WebshareProxyProvider(ProxyProvider):
    name = "webshare"
    def __init__(self):
        self.username = os.environ.get("WEBSHARE_USERNAME", "")
        self.password = os.environ.get("WEBSHARE_PASSWORD", "")
        self.endpoint = os.environ.get("WEBSHARE_ENDPOINT", "")
        self.port = int(os.environ.get("WEBSHARE_PORT", "10000"))
    def load(self) -> List[ProxyNode]:
        if not self.username or not self.password or not self.endpoint:
            logger.warning("WebshareProxyProvider: credentials or endpoint not set")
            return []
        return [ProxyNode(proxy_id=f"webshare_{self.endpoint}_{self.port}", host=self.endpoint,
                          port=self.port, protocol="socks5", username=self.username,
                          password=self.password, url=f"socks5://{self.username}:{self.password}@{self.endpoint}:{self.port}",
                          provider=self.name)]

def get_proxy_provider(provider_name: Optional[str] = None) -> ProxyProvider:
    name = (provider_name or PROXY_PROVIDER).lower()
    if name == "decodo": return DecodoProxyProvider()
    if name == "webshare": return WebshareProxyProvider()
    return FileProxyProvider()

class ProxyManager:
    def __init__(self, proxy_file: Optional[str] = None, provider_name: Optional[str] = None):
        self.proxy_file: str = proxy_file or "proxies.txt"
        self.provider: ProxyProvider = get_proxy_provider(provider_name)
        self.proxies: List[Dict[str, Any]] = []
        self.working_proxies: List[Dict[str, Any]] = []
        self.failed_proxies: Dict[str, int] = {}
        self.last_rotation: Dict[str, float] = {}
        self.count: int = 0
        self.working_count: int = 0
        self._testing_thread: Optional[threading.Thread] = None
        self._stop_testing: bool = False
        self._tested_count: int = 0
        self._working_found: int = 0
        self._testing_active: bool = False
        self._testing_progress: Dict[str, Any] = {"tested": 0, "working": 0, "failed": 0, "percent": 0.0, "status": "idle", "error": None}
        self._lock: threading.Lock = threading.Lock()
        self._session: requests.Session = self._create_session()
        self._load_proxies()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=1, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        session.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=CONNECTION_POOL_SIZE, pool_maxsize=CONNECTION_POOL_SIZE))
        session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=CONNECTION_POOL_SIZE, pool_maxsize=CONNECTION_POOL_SIZE))
        return session

    def _load_proxies(self) -> None:
        nodes = self.provider.load()
        self.proxies = [{"addr": n.host, "host": n.host, "port": n.port, "proxy_type": n.protocol,
                         "type": n.protocol, "username": n.username, "password": n.password,
                         "url": n.url, "added_at": n.added_at, "proxy_id": n.proxy_id, "latency": n.latency} for n in nodes]
        self.count = len(self.proxies)
        logger.info(f"Loaded {self.count} proxies from {self.provider.name} provider")
        if self.count == 0:
            logger.warning("No proxies found. Downloading from sources...")
            self._download_proxies()

    def _download_proxies(self) -> None:
        provider_name = os.environ.get("PROXY_PROVIDER", "file").lower()
        if provider_name in ("decodo", "webshare"):
            self.provider = get_proxy_provider(provider_name)
            nodes = self.provider.load()
            for node in nodes:
                self.proxies.append({"addr": node.host, "host": node.host, "port": node.port, "proxy_type": node.protocol,
                                     "type": node.protocol, "username": node.username, "password": node.password,
                                     "url": node.url, "added_at": node.added_at, "proxy_id": node.proxy_id, "latency": node.latency})
            self.count = len(self.proxies)
            logger.info(f"Downloaded {self.count} proxies from {provider_name} provider")

    def _test_proxy_sync(self, proxy: Dict[str, Any], timeout: Optional[float] = None) -> bool:
        try:
            start = time.time()
            resp = self._session.get("https://core.telegram.org", proxies={"http": proxy["url"], "https": proxy["url"]},
                                     timeout=timeout or PROXY_TEST_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                proxy["latency"] = (time.time() - start) * 1000
                return True
        except Exception:
            pass
        return False

    def get_testing_progress(self) -> Dict[str, Any]:
        with self._lock: return self._testing_progress.copy()

    def start_background_testing(self, max_workers: Optional[int] = None, progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None, initial_batch: Optional[int] = None) -> None:
        if self._testing_active: return
        max_workers = max_workers or MAX_WORKERS_DEFAULT
        initial_batch = initial_batch or min(100, len(self.proxies))
        self._stop_testing = False
        self._testing_active = True
        self._tested_count = 0
        self._working_found = 0

        def _update_progress(finished: bool = False) -> None:
            with self._lock:
                total = len(self.proxies)
                tested = self._tested_count
                working = self._working_found
                self._testing_progress.update({"tested": tested, "working": working, "failed": tested - working,
                                               "percent": (tested / total * 100) if total > 0 else 0.0, "status": "complete" if finished else "testing", "error": None})
                if progress_callback:
                    try: progress_callback(self._testing_progress.copy())
                    except Exception as e: logger.error(f"Progress callback error: {e}")

        def _test_batch(proxies_to_test: List[Dict[str, Any]]) -> None:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_proxy = {executor.submit(self._test_proxy_sync, p): p for p in proxies_to_test}
                for future in as_completed(future_to_proxy):
                    if self._stop_testing:
                        for f in future_to_proxy: f.cancel()
                        break
                    proxy = future_to_proxy[future]
                    try:
                        result = future.result(timeout=PROXY_TEST_TIMEOUT_SLOW)
                        with self._lock:
                            self._tested_count += 1
                            if result:
                                if proxy not in self.working_proxies: self.working_proxies.append(proxy)
                                self._working_found += 1
                                self.working_count = len(self.working_proxies)
                            else:
                                self.failed_proxies[proxy["url"]] = self.failed_proxies.get(proxy["url"], 0) + 1
                    except (FuturesTimeoutError, Exception):
                        with self._lock:
                            self._tested_count += 1
                            self.failed_proxies[proxy["url"]] = self.failed_proxies.get(proxy["url"], 0) + 1
                    if self._tested_count % TEST_BATCH_SIZE == 0: _update_progress()
            _update_progress()

        def _run_testing() -> None:
            try:
                if not self.proxies:
                    with self._lock: self._testing_progress.update({"status": "error", "error": "No proxies to test"})
                    _update_progress(finished=True)
                    return
                initial = self.proxies[:initial_batch]
                if initial: _test_batch(initial)
                remaining = self.proxies[initial_batch:]
                if remaining and not self._stop_testing: _test_batch(remaining)
            except Exception as e:
                logger.error(f"Proxy testing error: {e}")
                with self._lock: self._testing_progress.update({"status": "error", "error": str(e)})
            finally:
                self._testing_active = False
                _update_progress(finished=True)

        self._testing_thread = threading.Thread(target=_run_testing, name="ProxyTester", daemon=True)
        self._testing_thread.start()

    def stop_background_testing(self) -> None:
        self._stop_testing = True
        if self._testing_thread and self._testing_thread.is_alive(): self._testing_thread.join(timeout=2.0)
        self._testing_active = False

    def test_all(self, max_workers: Optional[int] = None, progress_callback: Optional[Callable[[int, int, int], None]] = None) -> None:
        if not self.proxies: return
        max_workers = max_workers or MAX_WORKERS_DEFAULT
        total = len(self.proxies)
        self.working_proxies.clear()
        self.working_count = 0
        tested = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {executor.submit(self._test_proxy_sync, p): p for p in self.proxies}
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    if future.result(timeout=PROXY_TEST_TIMEOUT_SLOW):
                        self.working_proxies.append(proxy)
                except Exception: pass
                tested += 1
                if tested % 10 == 0 and progress_callback: progress_callback(tested, len(self.working_proxies), total)
        self.working_count = len(self.working_proxies)
        if progress_callback: progress_callback(tested, self.working_count, total)
        logger.info(f"Proxy test complete: {self.working_count}/{total} working")

    def get_proxy(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self.working_proxies: return None
            available = [p for p in self.working_proxies if not self.should_rotate(p)]
            if not available:
                for p in self.working_proxies: self.rotate(p)
                available = self.working_proxies
            if not available: return None
            try:
                max_lat = max(p.get("latency", 5000.0) for p in available) or 1.0
                weights = [max(1.0, max_lat - p.get("latency", 5000.0) + 1.0) for p in available]
                return random.choices(available, weights=weights, k=1)[0]
            except Exception:
                return random.choice(available)

    def get_telethon_proxy(self) -> Optional[Tuple[str, str, int, bool, Optional[str], Optional[str]]]:
        p = self.get_proxy()
        if not p: return None
        ptype_map = {"http": "http", "socks4": "socks4", "socks5": "socks5"}
        return (ptype_map.get(p["type"], "socks5"), p["host"], p["port"], True, p.get("username"), p.get("password"))

    def mark_failed(self, proxy: Dict[str, Any]) -> None:
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        with self._lock:
            self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
            if self.failed_proxies[url] >= MAX_PROXY_FAILURES and proxy in self.working_proxies:
                self.working_proxies.remove(proxy)
                self.working_count = len(self.working_proxies)

    def rotate(self, proxy: Dict[str, Any]) -> None:
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        self.last_rotation[url] = time.time()

    def should_rotate(self, proxy: Dict[str, Any]) -> bool:
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        last = self.last_rotation.get(url, 0.0)
        return (time.time() - last) > PROXY_ROTATION_WINDOW

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"total": self.count, "working": self.working_count, "failed": len(self.failed_proxies),
                    "testing": self._testing_active, "progress": self._testing_progress.copy()}

    def clear_working(self) -> None:
        with self._lock: self.working_proxies.clear(); self.working_count = 0
    def reload(self) -> None: self._load_proxies()
    def wait_for_first_proxy(self, timeout: float = 30.0) -> bool:
        start = time.time()
        while self._testing_active and self.working_count == 0:
            if time.time() - start > timeout: return False
            time.sleep(0.5)
        return self.working_count > 0

    @property
    def raw_proxies(self) -> List[str]: return [p["url"] for p in self.proxies]
    def parse_proxy_string(self, proxy_str: str) -> Optional[Dict[str, Any]]: return self._dict_to_proxy_node(proxy_str)
    @staticmethod
    def _dict_to_proxy_node(line: str) -> Optional[Dict[str, Any]]:
        line = line.strip()
        if not line or line.startswith("#"): return None
        protocol, username, password = "http", None, None
        if "://" in line:
            proto_part, rest = line.split("://", 1)
            if proto_part.lower() in ("http", "https", "socks4", "socks5"): protocol, line = proto_part.lower(), rest
        parts = line.split(":")
        if len(parts) < 2: return None
        try:
            host, port = parts[0], int(parts[1])
            if len(parts) >= 4: username, password = parts[2], parts[3]
        except (ValueError, IndexError): return None
        from urllib.parse import quote
        enc_user, enc_pass = ((quote(u, safe="") if u else None) for u in (username, password))
        return {"addr": host, "host": host, "port": port, "proxy_type": protocol, "type": protocol,
                "username": username, "password": password,
                "url": f"{protocol}://{enc_user}:{enc_pass}@{host}:{port}" if enc_user and enc_pass else f"{protocol}://{host}:{port}"}
    def get_secured_proxy(self) -> Optional[Dict[str, Any]]: return self.get_proxy()
    def flag_proxy_failure(self, proxy: Dict[str, Any]) -> None: self.mark_failed(proxy)

class ProxyLeaseManager:
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self.proxy_nodes: Dict[str, ProxyNode] = {}
        self.proxy_cooldown: Set[str] = set()
        self.account_cooldown: Dict[str, float] = {}
        self._reaper_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._liveness_check: Optional[Callable[[str, Optional[str], Optional[str]], Any]] = None
        self.stats: Dict[str, int] = {"total_acquires": 0, "total_releases": 0, "cooldown_activations": 0, "current_active_leases": 0, "stale_leases_reaped": 0}

    def set_liveness_check(self, hook: Callable[[str, Optional[str], Optional[str]], Any]) -> None:
        """Register an async hook(leased_to, proxy_id, lease_id) -> bool used by the
        reaper to decide whether a long-held lease still backs a live session."""
        self._liveness_check = hook

    def _proxy_id_from_record(self, proxy_dict: Dict[str, Any]) -> Optional[str]:
        proxy_id = proxy_dict.get("proxy_id")
        if proxy_id: return str(proxy_id)
        url = proxy_dict.get("url")
        if url: return str(url)
        host = proxy_dict.get("host", proxy_dict.get("addr", ""))
        port = proxy_dict.get("port")
        return f"{host}:{port}" if host and port else None

    async def start(self) -> None:
        if self._is_running: return
        self._is_running = True
        await self._sync_proxies()
        self._reaper_task = asyncio.create_task(self._auto_reaper_loop())
        logger.info("ProxyLeaseManager started with auto-reaper")

    async def stop(self) -> None:
        self._is_running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try: await self._reaper_task
            except asyncio.CancelledError: pass
        self._reaper_task = None
        logger.info("ProxyLeaseManager stopped")

    async def _sync_proxies(self) -> None:
        working = list(self.proxy_manager.working_proxies)
        incoming: Dict[str, Dict[str, Any]] = {}
        for pd in working:
            pid = self._proxy_id_from_record(pd)
            if pid: incoming[pid] = pd
        async with self._condition:
            for pid, pd in incoming.items():
                node = self.proxy_nodes.get(pid)
                if node is None:
                    node = ProxyNode(proxy_id=pid, host=pd.get("host", pd.get("addr", "")), port=int(pd.get("port", 0)),
                                     protocol=pd.get("type", pd.get("proxy_type", "socks5")), username=pd.get("username"),
                                     password=pd.get("password"), url=pd.get("url", ""), latency=float(pd.get("latency", 5000.0)),
                                     provider=pd.get("provider", getattr(self.proxy_manager.provider, "name", "unknown")))
                    self.proxy_nodes[pid] = node
                else:
                    node.latency = float(pd.get("latency", node.latency))
                    node.health_state = "healthy"
            for pid in list(self.proxy_nodes):
                if pid in incoming: continue
                if not self.proxy_nodes[pid].is_leased:
                    self.proxy_nodes.pop(pid, None)
                    self.proxy_cooldown.discard(pid)

    async def _auto_reaper_loop(self) -> None:
        while self._is_running:
            try:
                await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)
                # Refresh the node pool continuously: start() snapshots BEFORE
                # background proxy testing has found anything, so without this
                # the pool stays empty until some consumer calls acquire_proxy.
                await self._sync_proxies()
                now = time.time()
                expired_proxies = [pid for pid in list(self.proxy_cooldown) if not self.proxy_nodes.get(pid) or not self.proxy_nodes[pid].is_in_cooldown()]
                expired_accounts = [ph for ph, cu in list(self.account_cooldown.items()) if now >= cu]
                # Snapshot stale-lease candidates under the lock, verify liveness
                # outside it (the hook takes SessionManager's lock).
                stale_candidates: List[Tuple[ProxyNode, str, float]] = []
                async with self._condition:
                    for node in self.proxy_nodes.values():
                        if node.is_leased and node.lease_time and (now - node.lease_time) > PROXY_LEASE_STALE_TTL:
                            stale_candidates.append((node, node.lease_id or "", now - node.lease_time))
                    for pid in expired_proxies: self.proxy_cooldown.discard(pid)
                    for ph in expired_accounts: self.account_cooldown.pop(ph, None)
                    self._condition.notify_all()
                for node, snapshot_lease_id, lease_age in stale_candidates:
                    live = True
                    if self._liveness_check is not None:
                        try:
                            live = bool(await self._liveness_check(node.leased_to or "", node.proxy_id, node.lease_id or ""))
                        except Exception:
                            live = True  # cannot verify: never force-release on hook failure
                    if live:
                        continue
                    async with self._condition:
                        # Re-verify: only reap if the SAME lease is still held.
                        if not node.is_leased or node.lease_id != snapshot_lease_id:
                            continue
                        held_by = node.leased_to
                        node.release()
                        self.stats["current_active_leases"] = max(0, self.stats["current_active_leases"] - 1)
                        self.stats["stale_leases_reaped"] += 1
                        self._condition.notify_all()
                    logger.warning(
                        "PROXY_LEASE_REAPED | proxy=%s | held_by=%s | lease=%s | age=%.0fs | no live session",
                        node.safe_label(), held_by or "?", snapshot_lease_id or "?", lease_age,
                    )
            except asyncio.CancelledError: break
            except Exception as e:
                logger.error(f"Auto-Reaper error: {e}")
                await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)

    async def acquire_proxy(self, phone: str, timeout: float = 10.0, allow_reserved: bool = False) -> Optional[Dict[str, Any]]:
        if not self._is_running: return None
        await self._sync_proxies()
        deadline = time.monotonic() + max(0.0, timeout)
        async with self._condition:
            while self._is_running:
                cooldown_until = self.account_cooldown.get(phone)
                if cooldown_until is not None:
                    remaining = cooldown_until - time.time()
                    if remaining > 0:
                        wait_for = min(remaining, max(0.0, deadline - time.monotonic()))
                        if wait_for <= 0: return None
                        try: await asyncio.wait_for(self._condition.wait(), timeout=wait_for)
                        except asyncio.TimeoutError:
                            if time.monotonic() >= deadline: return None
                            continue
                    else: self.account_cooldown.pop(phone, None)
                # LOGIN RESERVE: normal operations may never lease into the
                # buffer kept for new logins (logins pass allow_reserved=True).
                reserve = 0 if allow_reserved else self.login_reserve_limit()
                if not allow_reserved and self.get_available_count() <= reserve:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0: return None
                    try: await asyncio.wait_for(self._condition.wait(), timeout=min(remaining, 5.0))
                    except asyncio.TimeoutError:
                        if time.monotonic() >= deadline: return None
                    continue
                for pid, node in self.proxy_nodes.items():
                    if pid in self.proxy_cooldown or node.is_in_cooldown() or node.is_leased: continue
                    lease_id = uuid.uuid4().hex[:16]
                    if not node.acquire(phone, lease_id): continue
                    self.stats["total_acquires"] += 1
                    self.stats["current_active_leases"] += 1
                    proxy = node.to_telethon_proxy()
                    if proxy is None:
                        node.release(); self.stats["current_active_leases"] = max(0, self.stats["current_active_leases"] - 1)
                        continue
                    # Expose the URL so lifecycle logs show which proxy was
                    # actually leased (Telethon ignores the extra key).
                    proxy["url"] = node.url
                    proxy["__proxy_id"] = pid; proxy["__lease_id"] = lease_id
                    return proxy
                remaining = deadline - time.monotonic()
                if remaining <= 0: return None
                try: await asyncio.wait_for(self._condition.wait(), timeout=min(remaining, 5.0))
                except asyncio.TimeoutError:
                    if time.monotonic() >= deadline: return None
        return None

    async def release_proxy(self, *, proxy_url: Optional[str], phone: str, proxy_id: Optional[str] = None, lease_id: Optional[str] = None, should_cooldown: bool = False, cooldown_reason: str = "") -> None:
        async with self._condition:
            node = self.proxy_nodes.get(proxy_id) if proxy_id else None
            if node is None and proxy_url:
                for c in self.proxy_nodes.values():
                    if c.url == proxy_url: node = c; break
            if node is None: return
            if not node.is_leased or node.leased_to != phone: return
            if lease_id is not None and node.lease_id != lease_id: return
            if should_cooldown:
                # Error path: rest the proxy AND hold the account back so it is
                # not hammered again immediately.
                node.put_in_cooldown(duration_seconds=humanized_proxy_cooldown())
                self.proxy_cooldown.add(node.proxy_id)
                self.account_cooldown[phone] = time.time() + ACCOUNT_COOLDOWN_SECONDS
                self.stats["cooldown_activations"] += 1
                self.stats["current_active_leases"] = max(0, self.stats["current_active_leases"] - 1)
            else:
                # Normal disconnect: free the lease, then give the proxy its
                # human rest window (3-5 min + micro-jitter) before it may
                # serve another account.
                node.release()
                node.start_cooldown(duration_seconds=humanized_proxy_cooldown())
                self.proxy_cooldown.add(node.proxy_id)
                self.stats["total_releases"] += 1
                self.stats["current_active_leases"] = max(0, self.stats["current_active_leases"] - 1)
            self._condition.notify_all()

    def get_available_count(self) -> int:
        return sum(1 for n in self.proxy_nodes.values() if not n.is_leased and not n.is_in_cooldown() and n.proxy_id not in self.proxy_cooldown)

    def login_reserve_limit(self) -> int:
        """How many proxies are held back for new logins. Scaled down on small
        pools so normal work is never fully starved: 10+ nodes -> 2 reserved,
        3-4 nodes -> 1, fewer -> 0."""
        total = len(self.proxy_nodes)
        if total >= 5:
            return max(0, min(LOGIN_RESERVED_PROXIES, total - 2))
        if total >= 3:
            return 1
        return 0

    def usable_available_count(self) -> int:
        """Proxies normal operations may actually consume (login reserve excluded)."""
        return max(0, self.get_available_count() - self.login_reserve_limit())

    async def get_usable_count_async(self) -> int:
        async with self._condition: return self.usable_available_count()

    async def get_available_count_async(self) -> int:
        async with self._condition: return self.get_available_count()

    async def get_stats(self) -> Dict[str, Any]:
        async with self._condition:
            return {**self.stats, "available_proxies": self.get_available_count(),
                    "total_proxy_nodes": len(self.proxy_nodes), "proxies_in_cooldown": len(self.proxy_cooldown),
                    "accounts_in_cooldown": len(self.account_cooldown)}

# ────────────────────────────────────────────────────────────────
# 2. ACCOUNT LEASE LAYER (Runtime Ownership & Eligibility)
# ────────────────────────────────────────────────────────────────
class AccountState(str, enum.Enum):
    AVAILABLE = "available"; RESERVED = "reserved"; BUSY = "busy"
    QUARANTINED = "quarantined"; TERMINAL = "terminal"; DB_TERMINAL = "db_terminal"

@dataclass
class AccountLease:
    phone: str; session_fingerprint: str; owner: str; worker_id: str; module: str
    proxy_id: Optional[str] = None
    acquired_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)
    expires_at: float = field(default_factory=lambda: time.time() + 3600)
    lease_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    def is_expired(self) -> bool: return time.time() > self.expires_at
    def touch(self) -> None: self.last_activity = time.time(); self.expires_at = time.time() + 3600

class AccountLeaseManager:
    def __init__(self, db: SuiteDatabase, lease_ttl: float = 3600.0):
        self.db = db; self._lease_ttl = lease_ttl; self._lock = asyncio.Lock()
        self._leases: Dict[str, AccountLease] = {}; self._states: Dict[str, AccountState] = {}
        self._reaper_task: Optional[asyncio.Task] = None; self._is_running = False
        self._stats: Dict[str, Any] = {"leases_active": 0, "leases_acquired": 0, "leases_released": 0, "leases_expired": 0, "quarantines": 0}

    async def start(self) -> None:
        if self._is_running: return
        self._is_running = True; self._reaper_task = asyncio.create_task(self._reaper_loop())
    async def stop(self) -> None:
        self._is_running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try: await self._reaper_task
            except asyncio.CancelledError: pass
        self._reaper_task = None
        async with self._lock: self._leases.clear(); self._states.clear()

    async def _reaper_loop(self) -> None:
        while self._is_running:
            try:
                await asyncio.sleep(30)
                expired = [pk for pk, l in list(self._leases.items()) if l.is_expired()]
                async with self._lock:
                    for pk in expired:
                        self._leases.pop(pk, None); self._states[pk] = AccountState.AVAILABLE; self._stats["leases_expired"] += 1
            except asyncio.CancelledError: break
            except Exception as e: logger.error(f"Reaper error: {e}")

    @staticmethod
    def normalize_phone(phone: str) -> str: return "".join(c for c in str(phone) if c.isdigit())
    def _key(self, phone: str) -> str: return self.normalize_phone(phone)

    async def is_eligible(self, phone: str) -> bool:
        record = await self.db.get_session_by_phone_async(self._key(phone))
        if not record: return False
        return str(record.get("status", "")).lower() in ELIGIBLE_DB_STATUSES

    async def filter_eligible(self, accounts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [acc for acc in accounts if str(acc.get("status", "")).lower() in ELIGIBLE_DB_STATUSES]

    async def acquire(self, phone: str, *, module: str = "unknown", worker_id: Optional[str] = None, timeout: float = 30.0, skip_if_busy: bool = False) -> Optional[AccountLease]:
        clean_phone = self._key(phone)
        owner = f"{module}:{worker_id or uuid.uuid4().hex[:8]}"
        async with self._lock:
            existing = self._leases.get(clean_phone)
            state = self._states.get(clean_phone, AccountState.AVAILABLE)
            if existing and not existing.is_expired() and state in (AccountState.BUSY, AccountState.RESERVED):
                if skip_if_busy: return None
            if state == AccountState.QUARANTINED: return None

        deadline = time.time() + timeout
        while True:
            async with self._lock:
                existing = self._leases.get(clean_phone)
                state = self._states.get(clean_phone, AccountState.AVAILABLE)
                if state in (AccountState.QUARANTINED, AccountState.TERMINAL): return None
                if (existing is None or existing.is_expired()) and state in (AccountState.AVAILABLE, AccountState.TERMINAL):
                    record = await self.db.get_session_by_phone_async(clean_phone)
                    sess_str = record.get("session_string") or record.get("session", "") if record else ""
                    api_id = int(record.get("api_id", 0)) if record else 0
                    fingerprint = compute_session_fingerprint(sess_str, api_id) if sess_str else ""
                    lease = AccountLease(phone=clean_phone, session_fingerprint=fingerprint, owner=owner,
                                         worker_id=worker_id or "", module=module)
                    self._leases[clean_phone] = lease; self._states[clean_phone] = AccountState.RESERVED
                    self._stats["leases_acquired"] += 1; self._stats["leases_active"] = len(self._leases)
                    return lease
                if existing and not existing.is_expired() and state in (AccountState.BUSY, AccountState.RESERVED):
                    if skip_if_busy: return None
                    if existing.owner == owner: existing.touch(); self._states[clean_phone] = AccountState.BUSY; return existing
                if time.time() > deadline: return None
            await asyncio.sleep(0.5)

    async def release(self, phone: str, owner: str) -> bool:
        async with self._lock:
            lease = self._leases.get(self._key(phone))
            if lease and lease.owner == owner:
                del self._leases[self._key(phone)]; self._states[self._key(phone)] = AccountState.AVAILABLE
                self._stats["leases_released"] += 1; self._stats["leases_active"] = len(self._leases)
                return True
            return False

    async def fail_account(self, phone: str, category: ErrorCategory, reason: str) -> None:
        clean_phone = self._key(phone)
        async with self._lock:
            if category in (ErrorCategory.AUTH_KEY_DUPLICATED, ErrorCategory.SESSION_REVOKED, ErrorCategory.AUTH_KEY_UNREGISTERED, ErrorCategory.ACCOUNT_BANNED):
                self._states[clean_phone] = AccountState.QUARANTINED; self._stats["quarantines"] += 1
            else:
                self._states[clean_phone] = AccountState.AVAILABLE
            self._leases.pop(clean_phone, None); self._stats["leases_active"] = len(self._leases)
        db_mappings = {ErrorCategory.AUTH_KEY_DUPLICATED: "auth_key_duplicated", ErrorCategory.AUTH_KEY_UNREGISTERED: "revoked",
                       ErrorCategory.SESSION_REVOKED: "revoked", ErrorCategory.ACCOUNT_BANNED: "banned",
                       ErrorCategory.ACCOUNT_FLOOD: "failed", ErrorCategory.NETWORK_TIMEOUT: "failed", ErrorCategory.PROXY_ERROR: "failed"}
        db_status = db_mappings.get(category, "failed")
        try: await asyncio.to_thread(lambda: self.db.update_session_status(phone, db_status, None))
        except Exception as e: logger.error(f"DB status update failed in fail_account: {e}")

    async def get_state(self, phone: str) -> AccountState:
        async with self._lock: return self._states.get(self._key(phone), AccountState.AVAILABLE)
    async def is_busy(self, phone: str) -> bool:
        async with self._lock: return self._states.get(self._key(phone), AccountState.AVAILABLE) in (AccountState.BUSY, AccountState.RESERVED)
    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {**self._stats, "leases_active": len(self._leases),
                    "available": sum(1 for s in self._states.values() if s == AccountState.AVAILABLE),
                    "busy": sum(1 for s in self._states.values() if s in (AccountState.BUSY, AccountState.RESERVED)),
                    "quarantined": sum(1 for s in self._states.values() if s == AccountState.QUARANTINED),
                    "terminal": sum(1 for s in self._states.values() if s == AccountState.TERMINAL)}
    async def get_owned_phones(self) -> List[str]:
        async with self._lock: return list(self._leases.keys())

# ────────────────────────────────────────────────────────────────
# 3. SESSION LAYER (Client Lifecycle & Acquisition)
# ────────────────────────────────────────────────────────────────
class SessionLifecycleState(str, enum.Enum):
    UNKNOWN = "unknown"; AVAILABLE = "available"; RESERVED = "reserved"; BUSY = "busy"
    QUARANTINED = "quarantined"; DISCONNECTED = "disconnected"; TERMINAL = "terminal"
    LOGIN_PENDING = "login_pending"; OTP_WAITING = "otp_waiting"; TWOFA_WAITING = "twofa_waiting"

@dataclass
class SessionInfo:
    phone: str; session_fingerprint: str; status: str
    lifecycle: SessionLifecycleState = SessionLifecycleState.UNKNOWN
    client: Any = None; client_building: bool = False
    proxy_url: Optional[str] = None; proxy_id: Optional[str] = None; proxy_lease_id: Optional[str] = None
    owner: Optional[str] = None; worker_id: Optional[str] = None; reservation_id: Optional[str] = None
    lease_id: Optional[str] = None; client_id: Optional[str] = None
    connection_ts: Optional[float] = None; last_used_ts: float = field(default_factory=time.time)
    last_error: Optional[str] = None; creation_ts: float = field(default_factory=time.time)

@dataclass
class SessionLease:
    phone: str; session_fingerprint: str; client: Any; proxy_url: Optional[str]
    proxy_id: Optional[str]; proxy_lease_id: Optional[str]; owner: str; worker_id: Optional[str]
    lease_id: str; acquired_at: float = field(default_factory=time.time)
    proxy_record: Optional[dict] = None; proxy_should_cooldown: bool = False; proxy_cooldown_reason: str = ""
    released: bool = False

class SessionAlreadyOwnedError(Exception): pass

class SessionManager:
    def __init__(self, db: SuiteDatabase, proxy_manager: Optional[ProxyManager] = None, proxy_lease_manager: Optional[ProxyLeaseManager] = None, max_active_clients: int = 200, session_idle_ttl: float = 600.0):
        self.db = db; self.proxy_manager = proxy_manager; self.proxy_lease_manager = proxy_lease_manager
        self._lock = asyncio.Lock(); self._sessions: Dict[str, SessionInfo] = {}
        self._max_active_clients = max_active_clients; self._active_count = 0
        self._closed = False; self._session_idle_ttl = session_idle_ttl
        if self.proxy_lease_manager is not None:
            self.proxy_lease_manager.set_liveness_check(self._is_proxy_lease_live)

    async def _is_proxy_lease_live(self, phone: str, proxy_id: Optional[str], lease_id: Optional[str]) -> bool:
        """True while the phone still owns a live session that may be using this proxy lease."""
        async with self._lock:
            info = self._sessions.get(self._session_key(phone))
            if not info:
                return False
            owning = (SessionLifecycleState.BUSY, SessionLifecycleState.RESERVED,
                      SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING,
                      SessionLifecycleState.TWOFA_WAITING)
            if info.lifecycle not in owning:
                return False
            if (info.lifecycle == SessionLifecycleState.BUSY and lease_id
                    and info.proxy_lease_id and info.proxy_lease_id != lease_id):
                return False
            return True

    @staticmethod
    def _safe_proxy_label(proxy_url: Optional[str]) -> str:
        if not proxy_url: return ""
        try:
            parsed = urlparse(proxy_url); host = parsed.hostname or ""; port = parsed.port or ""
            return f"{host}:{port}" if host else "<proxy>"
        except Exception: return "<proxy>"

    @staticmethod
    def normalize_phone(phone: str) -> str: return "".join(c for c in str(phone) if c.isdigit())
    def _session_key(self, phone: str) -> str: return self.normalize_phone(phone)

    async def _log_lifecycle(self, event: str, *, phone: str = "", session_fp: str = "", module: str = "", worker_id: str = "", client_id: str = "", proxy_url: str = "", error: str = "", **extra: Any) -> None:
        logger.debug("SESSION_LIFECYCLE | event=%s | phone=%s | session_fp=%s | module=%s | worker=%s | client_id=%s | proxy=%s | error=%s | extra=%s",
                     event, phone, session_fp, module, worker_id, client_id, proxy_url, error, extra)

    @asynccontextmanager
    async def acquire(self, phone: str, *, module: str = "unknown", worker_id: Optional[str] = None, proxy_provider: Optional[Callable] = None, timeout: Optional[float] = None, auto_release: bool = True) -> AsyncIterator[Optional[SessionLease]]:
        if self._closed: raise RuntimeError("SessionManager is closed")
        clean_phone = self._session_key(phone)
        lease_owner_key = f"{module}:{worker_id or uuid.uuid4().hex[:8]}"
        reservation_id = uuid.uuid4().hex[:12]
        lease: Optional[SessionLease] = None; proxy_record: Optional[dict] = None; client: Any = None; yielded = False
        try:
            await self.cleanup_idle_sessions()
            async with self._lock:
                existing = self._sessions.get(clean_phone)
                if existing and existing.lifecycle in (SessionLifecycleState.BUSY, SessionLifecycleState.RESERVED, SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING):
                    raise SessionAlreadyOwnedError(f"Session +{clean_phone} is already owned by {existing.owner} (lifecycle={existing.lifecycle.value})")
                if existing and existing.lifecycle in (SessionLifecycleState.QUARANTINED, SessionLifecycleState.TERMINAL):
                    await self._log_lifecycle("SESSION_SKIPPED_TERMINAL", phone=clean_phone, session_fp=existing.session_fingerprint, module=module, worker_id=worker_id or "", error=f"lifecycle={existing.lifecycle.value}")
                    yield None; return
                if existing and existing.client is not None:
                    raise RuntimeError(f"Session +{clean_phone} has an unexpected retained client while lifecycle={existing.lifecycle.value}")
                if self._active_count >= self._max_active_clients:
                    raise RuntimeError(f"Maximum active clients ({self._max_active_clients}) reached")
                info = existing or SessionInfo(phone=clean_phone, session_fingerprint="", status="")
                self._sessions[clean_phone] = info
                info.lifecycle = SessionLifecycleState.RESERVED; info.owner = lease_owner_key
                info.worker_id = worker_id; info.reservation_id = reservation_id; info.lease_id = None
                info.last_used_ts = time.time()

            record = await self.db.get_session_by_phone_async(clean_phone)
            if not record: await self._rollback_reservation(clean_phone, lease_owner_key, reservation_id); yield None; return
            status = str(record.get("status", "")).lower()
            if status in TERMINAL_STATUSES:
                sess_str = record.get("session_string") or record.get("session", "")
                api_id = int(record.get("api_id", CONFIG["API_ID"]))
                fingerprint = compute_session_fingerprint(sess_str, api_id)
                async with self._lock:
                    info = self._sessions.get(clean_phone)
                    if info and info.owner == lease_owner_key:
                        info.lifecycle = SessionLifecycleState.TERMINAL; info.status = status
                        info.session_fingerprint = fingerprint; info.owner = None; info.worker_id = None
                        info.reservation_id = None; info.lease_id = None
                await self._log_lifecycle("SESSION_SKIPPED_TERMINAL", phone=clean_phone, session_fp=fingerprint, module=module, worker_id=worker_id or "", error=f"status={status}")
                yield None; return

            session_str = record.get("session_string") or record.get("session")
            if not session_str: await self._rollback_reservation(clean_phone, lease_owner_key, reservation_id); yield None; return
            api_id = int(record.get("api_id", CONFIG["API_ID"])); api_hash = str(record.get("api_hash", CONFIG["API_HASH"]))
            fingerprint = compute_session_fingerprint(session_str, api_id)
            device = record.get("device_metadata") or random.choice(DEVICE_PROFILES)

            # Honor the caller's timeout for the proxy-wait phase (dmsender
            # passes a short bound so workers stay responsive); callers that
            # omit it wait up to one full cooldown window.
            proxy_wait = PROXY_ACQUIRE_TIMEOUT if timeout is None else max(1.0, min(float(timeout), PROXY_ACQUIRE_TIMEOUT))
            if proxy_provider is not None: proxy_record = await proxy_provider(clean_phone)
            elif self.proxy_lease_manager is not None: proxy_record = await self.proxy_lease_manager.acquire_proxy(clean_phone, timeout=proxy_wait)
            if self.proxy_lease_manager is not None and proxy_record is None:
                await self._log_lifecycle("SESSION_WAITING_FOR_PROXY", phone=clean_phone, session_fp=fingerprint, module=module, worker_id=worker_id or "", error="proxy acquisition returned no lease")
                await self._rollback_reservation(clean_phone, lease_owner_key, reservation_id)
                yield None; return

            client = self._create_client(session_str=session_str, api_id=api_id, api_hash=api_hash, device=device, proxy=proxy_record)
            client_id = str(id(client))
            await self._log_lifecycle("SESSION_CLIENT_CREATED", phone=clean_phone, session_fp=fingerprint, module=module, worker_id=worker_id or "", client_id=client_id, proxy_url=(proxy_record or {}).get("url", "") if proxy_record else "")

            lease_id = uuid.uuid4().hex[:12]
            async with self._lock:
                info = self._sessions.get(clean_phone)
                if info is None: raise SessionAlreadyOwnedError(f"Session +{clean_phone} disappeared during acquisition")
                if info.owner != lease_owner_key: raise SessionAlreadyOwnedError(f"Session +{clean_phone} was acquired by another owner while I/O was in progress")
                if info.reservation_id != reservation_id: raise SessionAlreadyOwnedError(f"Session +{clean_phone} reservation changed while I/O was in progress")
                if info.client is not None: raise SessionAlreadyOwnedError(f"Session +{clean_phone} already has an active client")
                info.client = client; info.session_fingerprint = fingerprint; info.status = status
                info.lifecycle = SessionLifecycleState.BUSY; info.owner = lease_owner_key; info.worker_id = worker_id
                info.lease_id = lease_id; info.client_id = client_id; info.connection_ts = time.time()
                info.last_used_ts = time.time()
                info.proxy_url = proxy_record.get("url") if proxy_record else None
                info.proxy_id = proxy_record.get("__proxy_id") if proxy_record else None
                info.proxy_lease_id = proxy_record.get("__lease_id") if proxy_record else None
                self._active_count += 1

            lease = SessionLease(phone=clean_phone, session_fingerprint=fingerprint, client=client,
                                 proxy_url=proxy_record.get("url") if proxy_record else None,
                                 proxy_id=proxy_record.get("__proxy_id") if proxy_record else None,
                                 proxy_lease_id=proxy_record.get("__lease_id") if proxy_record else None,
                                 owner=lease_owner_key, worker_id=worker_id, lease_id=lease_id, proxy_record=proxy_record)
            await self._log_lifecycle("SESSION_ACQUIRED", phone=clean_phone, session_fp=fingerprint, module=module, worker_id=worker_id or "", client_id=client_id, proxy_url=lease.proxy_url or "", lease_id=lease_id)
            yielded = True
            yield lease
        except asyncio.CancelledError:
            if not yielded: await self._rollback_acquire_failure(clean_phone=clean_phone, owner_key=lease_owner_key, reservation_id=reservation_id, proxy_record=proxy_record, client=client)
            raise
        except BaseException:
            if not yielded: await self._rollback_acquire_failure(clean_phone=clean_phone, owner_key=lease_owner_key, reservation_id=reservation_id, proxy_record=proxy_record, client=client)
            raise
        finally:
            if lease is not None and auto_release: await self.release_lease(lease)

    async def reserve_login(self, phone: str, owner_key: str, client: Optional[Any] = None) -> bool:
        clean_phone = self._session_key(phone)
        record = await self.db.get_session_by_phone_async(clean_phone)
        if record and str(record.get("status", "")).lower() in TERMINAL_STATUSES: return False
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.lifecycle in (SessionLifecycleState.BUSY, SessionLifecycleState.RESERVED, SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING, SessionLifecycleState.QUARANTINED, SessionLifecycleState.TERMINAL): return False
            new_info = SessionInfo(phone=clean_phone, session_fingerprint="", status="login_pending", lifecycle=SessionLifecycleState.LOGIN_PENDING, client=client, owner=owner_key, worker_id=owner_key, lease_id=uuid.uuid4().hex[:12])
            if client is not None: new_info.client_id = str(id(client)); new_info.connection_ts = time.time()
            self._active_count += 1; self._sessions[clean_phone] = new_info
            return True

    async def set_login_stage(self, phone: str, owner_key: str, stage: SessionLifecycleState) -> bool:
        async with self._lock:
            info = self._sessions.get(self._session_key(phone))
            if not info or info.owner != owner_key: return False
            info.lifecycle = stage; return True

    async def release_login(self, phone: str, owner_key: str) -> None:
        clean_phone = self._session_key(phone)
        client_to_disconnect: Optional[Any] = None; proxy_url: Optional[str] = None; proxy_id: Optional[str] = None; proxy_lease_id: Optional[str] = None
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info: return
            if info.owner != owner_key: return
            if info.lifecycle not in (SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING): return
            client_to_disconnect = info.client; proxy_url = info.proxy_url; proxy_id = info.proxy_id; proxy_lease_id = info.proxy_lease_id
            if client_to_disconnect is not None: self._active_count = max(0, self._active_count - 1)
            self._sessions.pop(clean_phone, None)
        if client_to_disconnect is not None: await self._safe_disconnect_client(client_to_disconnect)
        if (proxy_url or proxy_id) and self.proxy_lease_manager is not None:
            try: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_url, proxy_id=proxy_id, lease_id=proxy_lease_id, phone=clean_phone)
            except Exception as exc: logger.error("LOGIN_PROXY_RELEASE_FAILED | phone=%s | error=%s", clean_phone, exc)

    async def build_login_client(self, phone: str, owner_key: str, *, session_str: str, api_id: int, api_hash: str, device: dict, proxy: Optional[dict] = None) -> Optional[Any]:
        clean_phone = self._session_key(phone)
        login_lifecycles = (SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info or info.owner != owner_key or info.lifecycle not in login_lifecycles: return None
            if info.client is not None: return info.client
            if info.client_building: return None
            info.client_building = True
        client: Optional[Any] = None; proxy_record = proxy
        try:
            if proxy_record is None and self.proxy_lease_manager is not None:
                # Logins may dip into the reserved proxy buffer.
                proxy_record = await self.proxy_lease_manager.acquire_proxy(clean_phone, timeout=PROXY_ACQUIRE_TIMEOUT, allow_reserved=True)
            if proxy_record is None: return None
            client = self._create_client(session_str=session_str, api_id=api_id, api_hash=api_hash, device=device, proxy=proxy_record)
            async with self._lock:
                info = self._sessions.get(clean_phone)
                if not info or info.owner != owner_key or info.lifecycle not in login_lifecycles: raise SessionAlreadyOwnedError(f"Login reservation lost for +{clean_phone}")
                if info.client is not None: raise SessionAlreadyOwnedError(f"Login client already exists for +{clean_phone}")
                info.client = client; info.proxy_url = proxy_record.get("url") if proxy_record else None
                info.proxy_id = proxy_record.get("__proxy_id") if proxy_record else None
                info.proxy_lease_id = proxy_record.get("__lease_id") if proxy_record else None
                info.client_id = str(id(client)); info.connection_ts = time.time(); info.last_used_ts = time.time()
                info.client_building = False; self._active_count += 1
                return client
        except BaseException:
            if client is not None: await self._safe_disconnect_client(client)
            if proxy_record and self.proxy_lease_manager is not None:
                try: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_record.get("url"), proxy_id=proxy_record.get("__proxy_id"), lease_id=proxy_record.get("__lease_id"), phone=clean_phone)
                except Exception as exc: logger.error("LOGIN_BUILD_PROXY_ROLLBACK_FAILED | phone=%s | error=%s", clean_phone, exc)
            raise
        finally:
            async with self._lock:
                info = self._sessions.get(clean_phone)
                if info and info.owner == owner_key: info.client_building = False

    async def is_owned(self, phone: str) -> bool:
        async with self._lock:
            info = self._sessions.get(self._session_key(phone))
            if not info: return False
            return info.lifecycle in (SessionLifecycleState.BUSY, SessionLifecycleState.RESERVED, SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING)

    async def _rollback_reservation(self, clean_phone: str, owner_key: str, reservation_id: Optional[str] = None) -> None:
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info or info.owner != owner_key or (reservation_id is not None and info.reservation_id != reservation_id): return
            if info.lifecycle not in (SessionLifecycleState.QUARANTINED, SessionLifecycleState.TERMINAL): info.lifecycle = SessionLifecycleState.AVAILABLE
            info.owner = None; info.worker_id = None; info.reservation_id = None; info.lease_id = None
            info.last_used_ts = time.time()

    async def _rollback_acquire_failure(self, *, clean_phone: str, owner_key: str, reservation_id: Optional[str], proxy_record: Optional[dict], client: Optional[Any]) -> None:
        was_counted = False
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.owner == owner_key and info.reservation_id == reservation_id:
                was_counted = info.client is not None
                info.client = None; info.proxy_url = None; info.proxy_id = None; info.proxy_lease_id = None
                info.owner = None; info.worker_id = None; info.reservation_id = None; info.lease_id = None
                info.client_id = None; info.connection_ts = None; info.last_used_ts = time.time()
                if info.lifecycle not in (SessionLifecycleState.QUARANTINED, SessionLifecycleState.TERMINAL): info.lifecycle = SessionLifecycleState.AVAILABLE
            if was_counted: self._active_count = max(0, self._active_count - 1)
        if client is not None: await self._safe_disconnect_client(client)
        if proxy_record and self.proxy_lease_manager is not None:
            try: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_record.get("url"), proxy_id=proxy_record.get("__proxy_id"), lease_id=proxy_record.get("__lease_id"), phone=clean_phone, should_cooldown=True, cooldown_reason="acquire_failure")
            except Exception as exc: logger.error("ACQUIRE_ROLLBACK_PROXY_RELEASE_FAILED | phone=%s | error=%s", clean_phone, exc)

    async def _release_lease(self, phone_key: str, owner_key: str, lease_id: Optional[str] = None, should_cooldown: bool = False) -> None:
        client_to_disconnect: Optional[Any] = None; proxy_url: Optional[str] = None; proxy_id: Optional[str] = None; proxy_lease_id: Optional[str] = None
        lifecycle_after_release = SessionLifecycleState.AVAILABLE; client_was_active = False
        async with self._lock:
            info = self._sessions.get(phone_key)
            if not info: return
            if info.owner != owner_key: return
            if lease_id is not None and info.lease_id != lease_id: return
            if info.lifecycle in (SessionLifecycleState.QUARANTINED, SessionLifecycleState.TERMINAL): lifecycle_after_release = info.lifecycle
            client_to_disconnect = info.client; proxy_url = info.proxy_url; proxy_id = info.proxy_id; proxy_lease_id = info.proxy_lease_id
            if client_to_disconnect is not None: client_was_active = True; self._active_count = max(0, self._active_count - 1)
            info.client = None; info.proxy_url = None; info.proxy_id = None; info.proxy_lease_id = None
            info.owner = None; info.worker_id = None; info.reservation_id = None; info.lease_id = None
            info.client_id = None; info.connection_ts = None; info.last_used_ts = time.time()
            info.lifecycle = lifecycle_after_release
        if client_to_disconnect is not None: await self._safe_disconnect_client(client_to_disconnect)
        if (proxy_url or proxy_id) and self.proxy_lease_manager is not None:
            try: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_url, proxy_id=proxy_id, lease_id=proxy_lease_id, phone=phone_key, should_cooldown=should_cooldown)
            except Exception as exc: logger.error("SESSION_PROXY_RELEASE_FAILED | phone=%s | proxy=%s | error=%s", phone_key, self._safe_proxy_label(proxy_url), exc)
        await self._log_lifecycle("SESSION_RELEASED", phone=phone_key, module=owner_key, client_id="", proxy_url=proxy_url or "", extra={"client_was_active": client_was_active, "active_count": self._active_count, "should_cooldown": should_cooldown})

    async def release_lease(self, lease: Optional[SessionLease]) -> None:
        if lease is None: return
        if lease.released: return
        phone_key = self._session_key(lease.phone)
        async with self._lock:
            info = self._sessions.get(phone_key)
            if not info: lease.released = True; return
            if info.owner != lease.owner: return
            if lease.lease_id is not None and info.lease_id != lease.lease_id: return
            lease.released = True
        await self._release_lease(phone_key, lease.owner, lease.lease_id, should_cooldown=lease.proxy_should_cooldown)

    async def mark_quarantined(self, phone: str, reason: str, category: ErrorCategory) -> None:
        clean_phone = self._session_key(phone); client_to_disconnect: Optional[Any] = None; proxy_url: Optional[str] = None
        proxy_id: Optional[str] = None; proxy_lease_id: Optional[str] = None; had_client = False; session_fp = ""
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info:
                session_fp = info.session_fingerprint; info.lifecycle = SessionLifecycleState.QUARANTINED
                info.last_error = f"{category.value}: {reason}"; client_to_disconnect = info.client
                proxy_url = info.proxy_url; proxy_id = info.proxy_id; proxy_lease_id = info.proxy_lease_id
                had_client = client_to_disconnect is not None
                info.client = None; info.proxy_url = None; info.proxy_id = None; info.proxy_lease_id = None
                info.owner = None; info.worker_id = None; info.lease_id = None; info.client_id = None
                if had_client: self._active_count = max(0, self._active_count - 1)
        if client_to_disconnect is not None: await self._safe_disconnect_client(client_to_disconnect)
        if (proxy_url or proxy_id) and self.proxy_lease_manager is not None:
            try: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_url, proxy_id=proxy_id, lease_id=proxy_lease_id, phone=clean_phone, should_cooldown=True, cooldown_reason="quarantine")
            except Exception as exc: logger.error("QUARANTINE_PROXY_RELEASE_FAILED | phone=%s | error=%s", clean_phone, exc)
        db_status = {ErrorCategory.AUTH_KEY_DUPLICATED: "auth_key_duplicated", ErrorCategory.SESSION_REVOKED: "revoked",
                     ErrorCategory.AUTH_KEY_UNREGISTERED: "revoked", ErrorCategory.ACCOUNT_BANNED: "banned",
                     ErrorCategory.UNAUTHORIZED: "revoked"}.get(category, "permanently_failed")
        try: await asyncio.to_thread(lambda: self.db.update_session_status(clean_phone, db_status, reason))
        except Exception as exc: logger.error("QUARANTINE_DB_UPDATE_FAILED | phone=%s | error=%s", clean_phone, exc)
        await self._log_lifecycle("SESSION_QUARANTINED", phone=clean_phone, session_fp=session_fp, module="session_manager", error=f"category={category.value}; reason={reason}")

    async def release(self, phone: str, module: str) -> None:
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info or info.owner != module: return
            lease_id = info.lease_id
        await self._release_lease(clean_phone, module, lease_id)

    async def release_proxy(self, phone: str, proxy_url: str) -> None:
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info: info.proxy_url = None
        if self.proxy_lease_manager and proxy_url: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_url, phone=clean_phone)
        elif self.proxy_manager and proxy_url:
            try: self.proxy_manager.mark_failed({"url": proxy_url})
            except Exception: pass

    def create_client(self, *, session_str: str, api_id: int = 0, api_hash: str = "", device: Optional[dict] = None, proxy: Optional[dict] = None) -> Any:
        return self._create_client(session_str=session_str, api_id=api_id or CONFIG["API_ID"], api_hash=api_hash or CONFIG["API_HASH"], device=device or {}, proxy=proxy)

    def _create_client(self, *, session_str: str, api_id: int, api_hash: str, device: dict, proxy: Optional[dict] = None) -> Any:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        # Telethon unpacks the proxy dict as **kwargs into Connection._parse_proxy,
        # so ONLY the six documented keys may be present. Lease metadata keys
        # (__proxy_id/__lease_id/url) raise TypeError instantly otherwise.
        clean_proxy: Optional[dict] = None
        if proxy:
            clean_proxy = {k: proxy[k] for k in
                           ("proxy_type", "addr", "port", "rdns", "username", "password")
                           if k in proxy}
        return TelegramClient(StringSession(session_str), api_id=api_id, api_hash=api_hash,
                              device_model=device.get("device_model", "PC 64bit"), system_version=device.get("system_version", "Windows 11"),
                              app_version=device.get("app_version", "4.8.4"), proxy=clean_proxy, entity_cache_limit=100,
                              sequential_updates=False, receive_updates=False, timeout=10.0, connection_retries=1, request_retries=1)

    async def _safe_disconnect_client(self, client: Optional[Any]) -> None:
        if not client: return
        try:
            if client.is_connected(): await asyncio.wait_for(client.disconnect(), timeout=3.0)
        except Exception as e: logger.debug(f"Safe disconnect error: {e}")

    def _update_db_status_sync(self, phone: str, status: str, reason: str) -> None:
        try:
            if status not in ("revoked", "banned"): self.db.mark_account_failed(phone, reason)
            self.db.update_session_status(phone, status)
        except Exception as e: logger.error(f"DB status update failed for {phone}: {e}")

    async def disconnect_all(self) -> int:
        snapshots: list = []
        async with self._lock:
            for phone_key, info in list(self._sessions.items()):
                snapshots.append((phone_key, info.client, info.proxy_url, info.proxy_id, info.proxy_lease_id))
                if info.lifecycle not in (SessionLifecycleState.QUARANTINED, SessionLifecycleState.TERMINAL): info.lifecycle = SessionLifecycleState.DISCONNECTED
                info.client = None; info.proxy_url = None; info.proxy_id = None; info.proxy_lease_id = None
                info.owner = None; info.worker_id = None; info.lease_id = None; info.client_id = None
                info.connection_ts = None; info.last_used_ts = time.time()
            self._active_count = 0
        for phone_key, client, proxy_url, proxy_id, proxy_lease_id in snapshots:
            if client is not None: await self._safe_disconnect_client(client)
            if (proxy_url or proxy_id) and self.proxy_lease_manager is not None:
                try: await self.proxy_lease_manager.release_proxy(proxy_url=proxy_url, proxy_id=proxy_id, lease_id=proxy_lease_id, phone=phone_key)
                except Exception as exc: logger.error("DISCONNECT_ALL_PROXY_RELEASE_FAILED | phone=%s | error=%s", phone_key, exc)
        return len(snapshots)

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            terminal = sum(1 for s in self._sessions.values() if s.lifecycle in (SessionLifecycleState.TERMINAL, SessionLifecycleState.QUARANTINED))
            busy = sum(1 for s in self._sessions.values() if s.lifecycle in (SessionLifecycleState.BUSY, SessionLifecycleState.RESERVED, SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING))
            return {"total_tracked": len(self._sessions), "active_clients": self._active_count, "busy": busy, "quarantined": terminal, "max_clients": self._max_active_clients}

    async def validate_invariants(self) -> Dict[str, Any]:
        async with self._lock:
            live_clients = sum(1 for s in self._sessions.values() if s.client is not None)
            owned_sessions = sum(1 for s in self._sessions.values() if s.owner is not None)
            violations = []
            for phone, info in self._sessions.items():
                if info.lifecycle == SessionLifecycleState.BUSY and info.client is None: violations.append(f"{phone}: BUSY without client")
                if info.client is None and info.proxy_url is not None and info.lifecycle not in (SessionLifecycleState.LOGIN_PENDING, SessionLifecycleState.OTP_WAITING, SessionLifecycleState.TWOFA_WAITING): violations.append(f"{phone}: proxy exists without client")
                if info.owner is None and info.lease_id is not None: violations.append(f"{phone}: lease_id without owner")
                if info.owner is None and info.lifecycle == SessionLifecycleState.BUSY: violations.append(f"{phone}: BUSY without owner")
            if live_clients != self._active_count: violations.append(f"active_count mismatch: counter={self._active_count}, actual={live_clients}")
            return {"ok": not violations, "active_count": self._active_count, "live_clients": live_clients, "owned_sessions": owned_sessions, "tracked_sessions": len(self._sessions), "violations": violations}

    async def cleanup_idle_sessions(self) -> int:
        now = time.time(); retiring = []
        async with self._lock:
            for phone_key, info in list(self._sessions.items()):
                if info.lifecycle == SessionLifecycleState.AVAILABLE and info.client is not None and (now - info.last_used_ts > self._session_idle_ttl):
                    cleanup_owner = f"__cleanup__:{uuid.uuid4().hex[:8]}"
                    info.lifecycle = SessionLifecycleState.RESERVED; info.owner = cleanup_owner; info.lease_id = None
                    retiring.append((phone_key, cleanup_owner, info.client))
        removed = 0
        for phone_key, cleanup_owner, client in retiring:
            await self._safe_disconnect_client(client)
            async with self._lock:
                info = self._sessions.get(phone_key)
                if info and info.owner == cleanup_owner and info.lifecycle == SessionLifecycleState.RESERVED:
                    info.client = None; info.proxy_url = None; info.owner = None; info.worker_id = None; info.lease_id = None
                    info.client_id = None; info.lifecycle = SessionLifecycleState.DISCONNECTED; info.last_used_ts = time.time()
                    self._active_count = max(0, self._active_count - 1); removed += 1
        return removed

# Backwards-compatible helper
async def safe_acquire_session(session_manager: SessionManager, phone: str, *, module: str = "unknown", worker_id: Optional[str] = None, proxy_provider=None, timeout: float = 30.0) -> Optional[SessionLease]:
    async with session_manager.acquire(phone, module=module, worker_id=worker_id, proxy_provider=proxy_provider, timeout=timeout, auto_release=False) as lease:
        return lease