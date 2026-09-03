"""
proxy_manager.py - Proxy management with rotation and health checking

Features:
- Fast concurrent proxy testing with connection pooling
- Real-time progress callbacks for UI updates
- Thread-safe operations with proper locking
- Automatic proxy download from online sources
- Rotation and failure tracking
- 🔥 NEW: ProxyLeaseManager for dynamic rolling batch architecture
"""

import asyncio
import logging
import random
import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from typing import List, Dict, Optional, Tuple, Callable, Any, Set
from urllib.parse import urlparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import CONFIG

logger = logging.getLogger(__name__)

# 🔥 FIX: Silence urllib3 console spam during proxy testing
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Color constants for console output
try:
    from colorama import Fore, Style
    G = Fore.GREEN
    Y = Fore.YELLOW
    R = Fore.RED
    C = Fore.CYAN
    B = Fore.BLUE
    RS = Style.RESET_ALL
except ImportError:
    G = Y = R = C = B = RS = ""

# Constants - Optimized for speed and reliability
PROXY_TEST_URL: str = "https://httpbin.org/ip"
PROXY_TEST_TIMEOUT: float = 5.0
PROXY_TEST_TIMEOUT_SLOW: float = 15.0
MAX_PROXY_FAILURES: int = 3
PROXY_ROTATION_WINDOW: float = 5 * 60
MIN_WORKING_PROXIES_TO_START: int = 1

# Performance tuning
TEST_BATCH_SIZE: int = 50
MAX_WORKERS_DEFAULT: int = 30
CONNECTION_POOL_SIZE: int = 20

# 🔥 NEW: Proxy Lease & Cooldown Configuration
PROXY_COOLDOWN_SECONDS: int = 600  # 10 minutes default cooldown
ACCOUNT_COOLDOWN_SECONDS: int = 900  # 15 minutes for accounts
COOLDOWN_CHECK_INTERVAL: float = 5.0  # Check expired cooldowns every 5 seconds


@dataclass
class ProxyNode:
    """Enterprise proxy node with lease tracking and cooldown state."""
    addr: str
    host: str
    port: int
    proxy_type: str
    username: Optional[str]
    password: Optional[str]
    url: str
    latency: float = 5000.0
    is_leased: bool = False
    leased_to: Optional[str] = None  # phone number
    lease_time: float = 0.0
    cooldown_until: float = 0.0
    failures: int = 0
    added_at: float = field(default_factory=time.time)
    
    def to_telethon_dict(self) -> Dict[str, Any]:
        """Convert to Telethon proxy dict format."""
        return {
            "proxy_type": self.proxy_type,
            "addr": self.addr,
            "port": self.port,
            "rdns": True,
            "username": self.username,
            "password": self.password
        }
    
    def is_in_cooldown(self) -> bool:
        """Check if proxy is currently in cooldown."""
        return time.time() < self.cooldown_until
    
    def acquire(self, phone: str) -> bool:
        """Attempt to acquire lease for this proxy."""
        now = time.time()
        if self.is_leased or self.is_in_cooldown():
            return False
        self.is_leased = True
        self.leased_to = phone
        self.lease_time = now
        return True
    
    def release(self) -> None:
        """Release the proxy lease."""
        self.is_leased = False
        self.leased_to = None
        self.lease_time = 0.0
    
    def put_in_cooldown(self, duration_seconds: int = PROXY_COOLDOWN_SECONDS) -> None:
        """Place proxy in cooldown."""
        self.release()
        self.cooldown_until = time.time() + duration_seconds
        logger.warning(f"Proxy {self.url} placed in cooldown until {datetime.fromtimestamp(self.cooldown_until).strftime('%H:%M:%S')}")


class ProxyLeaseManager:
    """
    🔥 Enterprise Proxy Lease & Cooldown Engine
    
    Features:
    - Zero-CPU blocking via asyncio.Condition
    - Dynamic concurrency based on available proxies
    - Automatic cooldown expiration with Auto-Reaper
    - Account-proxy binding for targeted bans
    """
    
    def __init__(self, proxy_manager: "ProxyManager"):
        self.proxy_manager = proxy_manager
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        
        # Proxy nodes indexed by URL for O(1) lookup
        self.proxy_nodes: Dict[str, ProxyNode] = {}
        
        # Cooldown pools
        self.proxy_cooldown: Set[str] = set()  # URLs of proxies in cooldown
        self.account_cooldown: Dict[str, float] = {}  # phone -> cooldown_until
        
        # Auto-reaper task
        self._reaper_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        # Statistics
        self.stats = {
            "total_acquires": 0,
            "total_releases": 0,
            "cooldown_activations": 0,
            "current_active_leases": 0
        }
    
    async def start(self) -> None:
        """Start the auto-reaper background task."""
        if self._is_running:
            return
        self._is_running = True
        self._reaper_task = asyncio.create_task(self._auto_reaper_loop())
        logger.info("🔥 ProxyLeaseManager started with auto-reaper")
    
    async def stop(self) -> None:
        """Stop the auto-reaper task."""
        self._is_running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
            self._reaper_task = None
        logger.info("ProxyLeaseManager stopped")
    
    def _sync_proxies(self) -> None:
        """Sync working proxies from ProxyManager into ProxyNodes."""
        for proxy_dict in self.proxy_manager.working_proxies:
            url = proxy_dict.get("url", "")
            if url and url not in self.proxy_nodes:
                node = ProxyNode(
                    addr=proxy_dict.get("host", ""),
                    host=proxy_dict.get("host", ""),
                    port=proxy_dict.get("port", 0),
                    proxy_type=proxy_dict.get("type", "socks5"),
                    username=proxy_dict.get("username"),
                    password=proxy_dict.get("password"),
                    url=url,
                    latency=proxy_dict.get("latency", 5000.0)
                )
                self.proxy_nodes[url] = node
    
    async def _auto_reaper_loop(self) -> None:
        """
        Background task that wakes up exactly when cooldowns expire.
        Uses efficient sleep with periodic checks.
        """
        while self._is_running:
            try:
                now = time.time()
                expired_proxies = []
                expired_accounts = []
                
                # Check expired proxy cooldowns
                for url in list(self.proxy_cooldown):
                    node = self.proxy_nodes.get(url)
                    if node and not node.is_in_cooldown():
                        expired_proxies.append(url)
                    elif node is None:
                        expired_proxies.append(url)  # Remove stale entries
                
                # Check expired account cooldowns
                for phone, cooldown_until in list(self.account_cooldown.items()):
                    if now >= cooldown_until:
                        expired_accounts.append(phone)
                
                # Remove expired entries and notify waiters
                if expired_proxies or expired_accounts:
                    async with self._condition:
                        for url in expired_proxies:
                            self.proxy_cooldown.discard(url)
                            logger.debug(f"Auto-Reaper: Proxy {url} cooldown expired")
                        
                        for phone in expired_accounts:
                            del self.account_cooldown[phone]
                            logger.debug(f"Auto-Reaper: Account +{phone} cooldown expired")
                        
                        # Wake up all waiting workers
                        self._condition.notify_all()
                
                # Sleep until next check or use smart sleep until earliest expiry
                await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-Reaper error: {e}")
                await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)
    
    async def acquire_proxy(self, phone: str) -> Optional[Dict[str, Any]]:
        """
        Acquire a proxy lease for the given account.
        Blocks efficiently (0% CPU) if no proxies are available.
        
        Returns:
            Telethon proxy dict or None if interrupted
        """
        self._sync_proxies()
        
        async with self._condition:
            while self._is_running:
                # Check if account is in cooldown
                if phone in self.account_cooldown:
                    remaining = self.account_cooldown[phone] - time.time()
                    if remaining > 0:
                        logger.debug(f"Account +{phone} in cooldown for {remaining:.0f}s more")
                        await self._condition.wait()
                        continue
                    else:
                        del self.account_cooldown[phone]
                
                # Find an available proxy
                for url, node in self.proxy_nodes.items():
                    if url in self.proxy_cooldown:
                        continue
                    if node.acquire(phone):
                        self.stats["total_acquires"] += 1
                        self.stats["current_active_leases"] += 1
                        logger.debug(f"Proxy {url} leased to +{phone}")
                        return node.to_telethon_dict()
                
                # No proxy available - wait efficiently
                logger.debug(f"No proxies available for +{phone}, waiting...")
                await self._condition.wait()
            
            return None
    
    async def release_proxy(self, proxy_url: str, phone: str, 
                           should_cooldown: bool = False,
                           cooldown_reason: str = "") -> None:
        """
        Release a proxy lease.
        
        Args:
            proxy_url: The URL of the proxy to release
            phone: The phone number that was using it
            should_cooldown: If True, place both proxy and account in cooldown
            cooldown_reason: Reason for cooldown (FloodWait, PeerFlood, Ban, etc.)
        """
        async with self._condition:
            node = self.proxy_nodes.get(proxy_url)
            if node:
                if should_cooldown:
                    node.put_in_cooldown(PROXY_COOLDOWN_SECONDS)
                    self.proxy_cooldown.add(proxy_url)
                    self.account_cooldown[phone] = time.time() + ACCOUNT_COOLDOWN_SECONDS
                    self.stats["cooldown_activations"] += 1
                    logger.warning(
                        f"Cooldown activated for +{phone} & proxy {proxy_url}: {cooldown_reason}"
                    )
                else:
                    node.release()
                
                self.stats["total_releases"] += 1
                self.stats["current_active_leases"] = max(0, self.stats["current_active_leases"] - 1)
                
                # Notify waiting workers
                self._condition.notify()
    
    def get_available_count(self) -> int:
        """Get count of available (not leased, not in cooldown) proxies."""
        self._sync_proxies()
        count = 0
        now = time.time()
        for url, node in self.proxy_nodes.items():
            if url not in self.proxy_cooldown and not node.is_leased and not node.is_in_cooldown():
                count += 1
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current lease manager statistics."""
        return {
            **self.stats,
            "available_proxies": self.get_available_count(),
            "proxies_in_cooldown": len(self.proxy_cooldown),
            "accounts_in_cooldown": len(self.account_cooldown)
        }


class ProxyManager:
    """Manages proxy list with rotation and health checking."""

    def __init__(self, proxy_file: Optional[str] = None) -> None:
        """Initialize ProxyManager with optional custom proxy file."""
        self.proxy_file: str = proxy_file or "proxies.txt"
        self.proxies: List[Dict[str, Any]] = []
        self.working_proxies: List[Dict[str, Any]] = []
        self.failed_proxies: Dict[str, int] = {}
        self.last_rotation: Dict[str, float] = {}
        
        # Counters with type hints
        self.count: int = 0
        self.working_count: int = 0
        
        # Background testing state
        self._testing_thread: Optional[threading.Thread] = None
        self._stop_testing: bool = False
        self._tested_count: int = 0
        self._working_found: int = 0
        self._testing_active: bool = False
        
        # Progress tracking (thread-safe)
        self._testing_progress: Dict[str, Any] = {
            "tested": 0,
            "working": 0,
            "failed": 0,
            "percent": 0.0,
            "status": "idle",
            "error": None
        }
        
        # Thread safety
        self._lock: threading.Lock = threading.Lock()
        
        # HTTP session for faster testing with connection pooling
        self._session: requests.Session = self._create_session()
        
        # Load proxies on init
        self._load_proxies()

    def _create_session(self) -> requests.Session:
        """Create optimized requests session with connection pooling."""
        session = requests.Session()
        
        # Retry strategy for transient failures
        retry = Retry(
            total=1,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=CONNECTION_POOL_SIZE,
            pool_maxsize=CONNECTION_POOL_SIZE
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def _load_proxies(self) -> None:
        """Load proxies from file with error handling."""
        try:
            with open(self.proxy_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            self.proxies = []
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                
                proxy = self._parse_proxy(line)
                if proxy:
                    self.proxies.append(proxy)
            
            self.count = len(self.proxies)
            logger.info(f"Loaded {self.count} proxies from {self.proxy_file}")
            
            if self.count == 0:
                logger.warning("No proxies found. Downloading from sources...")
                self._download_proxies()
                
        except FileNotFoundError:
            logger.warning(f"Proxy file not found: {self.proxy_file}")
            self.count = 0
            self._download_proxies()
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            self.count = 0

    def _parse_proxy(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a proxy line into a dictionary."""
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        
        # Default values
        proxy_type: str = "http"
        username: Optional[str] = None
        password: Optional[str] = None
        
        # Handle protocol prefix
        if "://" in line:
            protocol, rest = line.split("://", 1)
            if protocol.lower() in ["http", "https", "socks4", "socks5"]:
                proxy_type = protocol.lower()
                line = rest
        
        # Parse host:port[:user:pass]
        parts: List[str] = line.split(":")
        if len(parts) < 2:
            return None
        
        try:
            host: str = parts[0]
            port: int = int(parts[1])
            
            if len(parts) >= 4:
                username = parts[2]
                password = parts[3]
            
            return {
                "addr": host,
                "host": host,
                "port": port,
                "proxy_type": proxy_type,
                "type": proxy_type,
                "username": username,
                "password": password,
                "url": self._build_proxy_url(host, port, proxy_type, username, password),
                "added_at": time.time()
            }
        except (ValueError, IndexError):
            return None

    def _build_proxy_url(
        self,
        host: str,
        port: int,
        ptype: str,
        username: Optional[str] = None,
        password: Optional[str] = None
    ) -> str:
        """Build proxy URL for requests library."""
        if username and password:
            return f"{ptype}://{username}:{password}@{host}:{port}"
        return f"{ptype}://{host}:{port}"

    def _download_proxies(self) -> None:
        """Download proxies from 12+ extended enterprise online sources."""
        logger.info("Downloading proxies from multiple enterprise sources...")
        
        # 🔥 FEATURE: Multi-Source Categorized Scraper
        PROXY_SOURCES = {
            "http": [
                "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
                "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
                "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
                "https://raw.githubusercontent.com/Ian-Lusule/Proxies/main/proxies/http.txt",
            ],
            "socks4": [
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
                "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=all",
                "https://raw.githubusercontent.com/Ian-Lusule/Proxies/main/proxies/socks4.txt",
            ],
            "socks5": [
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
                "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all",
                "https://raw.githubusercontent.com/Ian-Lusule/Proxies/main/proxies/socks5.txt",
            ]
        }
        
        all_proxies: List[Dict[str, Any]] = []
        
        for ptype, sources in PROXY_SOURCES.items():
            for source in sources:
                try:
                    response = requests.get(source, timeout=10)
                    if response.status_code == 200:
                        lines = response.text.split("\n")
                        for line in lines:
                            line = line.strip()
                            if line and ":" in line and not line.startswith("#"):
                                proxy = self._parse_proxy(f"{ptype}://{line}")
                                if proxy:
                                    all_proxies.append(proxy)
                except Exception as e:
                    logger.debug(f"Failed to download from {source}: {e}")
        
        if all_proxies:
            try:
                # Remove duplicates based on URL
                unique_proxies = {p['url']: p for p in all_proxies}.values()
                self.proxies = list(unique_proxies)
                self.count = len(self.proxies)
                
                with open(self.proxy_file, "w", encoding="utf-8") as f:
                    for proxy in self.proxies:
                        f.write(f"{proxy['url']}\n")
                logger.info(f"Downloaded {self.count} unique proxies to {self.proxy_file}")
            except Exception as e:
                logger.error(f"Failed to save proxies: {e}")

    def get_proxy(self) -> Optional[Dict[str, Any]]:
        """Get a working proxy with Smart Latency-Based Weighted Rotation."""
        with self._lock:
            if not self.working_proxies:
                return None
            
            available: List[Dict[str, Any]] = [
                p for p in self.working_proxies 
                if not self.should_rotate(p)
            ]
            
            if not available:
                for p in self.working_proxies:
                    self.rotate(p)
                available = self.working_proxies
            
            # 🔥 FEATURE: Smart Weighted Random Selection (Faster proxies chosen more often)
            try:
                # Assign default high latency to proxies without a latency score
                for p in available:
                    if "latency" not in p:
                        p["latency"] = 5000.0
                
                max_latency = max(p["latency"] for p in available) or 1.0
                # Invert weights: lower latency = higher weight
                weights = [max(1.0, max_latency - p["latency"] + 1.0) for p in available]
                
                return random.choices(available, weights=weights, k=1)[0]
            except Exception:
                # Fallback in case of unexpected math error
                return random.choice(available) if available else None

    def get_telethon_proxy(self) -> Optional[Tuple[str, str, int, bool, Optional[str], Optional[str]]]:
        """
        Get proxy in Telethon format.
        
        Returns:
            Tuple: (proxy_type, host, port, rdns, username, password) or None
        """
        proxy = self.get_proxy()
        if not proxy:
            return None
        
        # Map proxy types for Telethon
        ptype_map: Dict[str, str] = {
            "http": "http",
            "socks4": "socks4", 
            "socks5": "socks5"
        }
        
        return (
            ptype_map.get(proxy["type"], "socks5"),
            proxy["host"],
            proxy["port"],
            True,  # rdns (reverse DNS)
            proxy.get("username"),
            proxy.get("password")
        )

    def _test_proxy_sync(self, proxy: Dict[str, Any], timeout: Optional[float] = None) -> bool:
        """Test a single proxy synchronously and measure Telegram latency."""
        test_timeout = timeout or PROXY_TEST_TIMEOUT
        
        try:
            proxies = {
                "http": proxy["url"],
                "https": proxy["url"]
            }
            
            start_time = time.time()
            
            # 🔥 FEATURE: Direct Telegram Validation (Rejects proxies that block Telegram)
            response = self._session.get(
                "https://core.telegram.org",
                proxies=proxies,
                timeout=test_timeout,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            )
            
            if response.status_code == 200:
                # 🔥 FEATURE: Record Latency for Smart Rotation
                proxy["latency"] = (time.time() - start_time) * 1000
                return True
                
        except Exception:
            pass  # Proxy failed
        
        return False

    async def _test_proxy_async(self, proxy: Dict[str, Any]) -> bool:
        """Test proxy in async context using executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._test_proxy_sync,
            proxy,
            PROXY_TEST_TIMEOUT
        )

    def get_testing_progress(self) -> Dict[str, Any]:
        """Get current testing progress (thread-safe copy)."""
        with self._lock:
            return self._testing_progress.copy()

    def start_background_testing(
        self,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        initial_batch: Optional[int] = None
    ) -> None:
        """
        Start testing proxies in background thread with real-time updates.
        
        Args:
            max_workers: Number of concurrent test threads
            progress_callback: Function(progress_dict) called on updates
            initial_batch: Test this many first, then continue in background
        """
        if self._testing_active:
            logger.debug("Proxy testing already active")
            return
        
        max_workers = max_workers or MAX_WORKERS_DEFAULT
        initial_batch = initial_batch or min(100, len(self.proxies))
        
        self._stop_testing = False
        self._testing_active = True
        self._tested_count = 0
        self._working_found = 0
        
        def _update_progress(finished: bool = False) -> None:
            """Update progress dict and call callback (thread-safe)."""
            with self._lock:
                total = len(self.proxies)
                tested = self._tested_count
                working = self._working_found
                failed = tested - working
                
                self._testing_progress.update({
                    "tested": tested,
                    "working": working,
                    "failed": failed,
                    "percent": (tested / total * 100) if total > 0 else 0.0,
                    "status": "complete" if finished else "testing",
                    "error": None
                })
                
                if progress_callback:
                    try:
                        progress_callback(self._testing_progress.copy())
                    except Exception as e:
                        logger.error(f"Progress callback error: {e}")

        def _test_batch(proxies_to_test: List[Dict[str, Any]]) -> None:
            """Test a batch of proxies with thread pool."""
            nonlocal max_workers
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_proxy = {
                    executor.submit(self._test_proxy_sync, proxy): proxy
                    for proxy in proxies_to_test
                }
                
                for future in as_completed(future_to_proxy):
                    if self._stop_testing:
                        # Cancel remaining futures
                        for f in future_to_proxy:
                            f.cancel()
                        break
                    
                    proxy = future_to_proxy[future]
                    
                    try:
                        result = future.result(timeout=PROXY_TEST_TIMEOUT_SLOW)
                        with self._lock:
                            self._tested_count += 1
                            if result:
                                if proxy not in self.working_proxies:
                                    self.working_proxies.append(proxy)
                                    self._working_found += 1
                                    self.working_count = len(self.working_proxies)
                            else:
                                url = proxy["url"]
                                self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
                    except FuturesTimeoutError:
                        with self._lock:
                            self._tested_count += 1
                            url = proxy["url"]
                            self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
                    except Exception:
                        with self._lock:
                            self._tested_count += 1
                    
                    # Update progress every TEST_BATCH_SIZE tests
                    if self._tested_count % TEST_BATCH_SIZE == 0:
                        _update_progress()
            
            # Final update for this batch
            _update_progress()

        def _run_testing() -> None:
            """Main testing loop executed in background thread."""
            try:
                if not self.proxies:
                    with self._lock:
                        self._testing_progress["status"] = "error"
                        self._testing_progress["error"] = "No proxies to test"
                    _update_progress(finished=True)
                    return
                
                # Test initial batch first
                initial_proxies = self.proxies[:initial_batch]
                if initial_proxies:
                    logger.info(f"Testing initial batch: {len(initial_proxies)} proxies")
                    _test_batch(initial_proxies)
                
                # Continue with remaining proxies in background
                remaining = self.proxies[initial_batch:]
                if remaining and not self._stop_testing:
                    logger.info(f"Testing remaining {len(remaining)} proxies in background")
                    _test_batch(remaining)
                
            except Exception as e:
                logger.error(f"Proxy testing error: {e}")
                with self._lock:
                    self._testing_progress["status"] = "error"
                    self._testing_progress["error"] = str(e)
                _update_progress(finished=True)
            finally:
                self._testing_active = False
                _update_progress(finished=True)
                logger.info(f"Proxy testing complete: {self.working_count}/{self.count} working")

        # Start background thread
        self._testing_thread = threading.Thread(
            target=_run_testing,
            name="ProxyTester",
            daemon=True
        )
        self._testing_thread.start()
        logger.info(f"Started proxy testing with {max_workers} workers")

    def stop_background_testing(self) -> None:
        """Stop background testing gracefully."""
        self._stop_testing = True
        if self._testing_thread and self._testing_thread.is_alive():
            self._testing_thread.join(timeout=2.0)
        self._testing_active = False

    def test_all(
        self,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, int], None]] = None
    ) -> None:
        """
        Test all proxies synchronously with progress updates.
        
        Args:
            max_workers: Number of concurrent workers
            progress_callback: Optional callback(tested, working, total)
        """
        if not self.proxies:
            logger.warning("No proxies to test")
            return
        
        max_workers = max_workers or MAX_WORKERS_DEFAULT
        total = len(self.proxies)
        logger.info(f"Testing {total} proxies with {max_workers} workers...")
        
        self.working_proxies = []
        self.working_count = 0
        tested = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self._test_proxy_sync, proxy): proxy
                for proxy in self.proxies
            }
            
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                
                try:
                    result = future.result(timeout=PROXY_TEST_TIMEOUT_SLOW)
                    if result:
                        self.working_proxies.append(proxy)
                except Exception:
                    pass
                
                tested += 1
                
                # Progress update every 10 tests
                if tested % 10 == 0 and progress_callback:
                    working = len(self.working_proxies)
                    progress_callback(tested, working, total)
                elif tested % 50 == 0:
                    working = len(self.working_proxies)
                    percent = tested / total * 100
                    print(f"\r  {G}Testing:{RS} {tested}/{total} ({percent:.1f}%) | {G}Working:{RS} {working}", end="", flush=True)
        
        # Final update
        self.working_count = len(self.working_proxies)
        if progress_callback:
            progress_callback(tested, self.working_count, total)
        
        print(f"\r  {G}Complete:{RS} {self.working_count}/{total} working proxies\n")
        logger.info(f"Proxy test complete: {self.working_count}/{total} working")

    def mark_failed(self, proxy: Dict[str, Any]) -> None:
        """Mark a proxy as failed and remove if over threshold."""
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        with self._lock:
            self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
            
            if self.failed_proxies[url] >= MAX_PROXY_FAILURES:
                if proxy in self.working_proxies:
                    self.working_proxies.remove(proxy)
                    self.working_count = len(self.working_proxies)
                logger.debug(f"Proxy {url} removed after {MAX_PROXY_FAILURES} failures")

    def rotate(self, proxy: Dict[str, Any]) -> None:
        """Mark proxy for rotation (reset usage timer)."""
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        self.last_rotation[url] = time.time()

    def should_rotate(self, proxy: Dict[str, Any]) -> bool:
        """Check if proxy should be rotated based on time window."""
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        last = self.last_rotation.get(url, 0.0)
        return (time.time() - last) > PROXY_ROTATION_WINDOW

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive proxy statistics."""
        with self._lock:
            return {
                "total": self.count,
                "working": self.working_count,
                "failed": len(self.failed_proxies),
                "testing": self._testing_active,
                "progress": self._testing_progress.copy()
            }

    def clear_working(self) -> None:
        """Clear working proxies list (useful for re-testing)."""
        with self._lock:
            self.working_proxies = []
            self.working_count = 0

    def reload(self) -> None:
        """Reload proxies from file."""
        self._load_proxies()

    def wait_for_first_proxy(self, timeout: float = 30.0) -> bool:
        """
        Wait for at least one working proxy to be found.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if a working proxy was found, False if timeout
        """
        start_time = time.time()
        
        while self._testing_active and self.working_count == 0:
            if time.time() - start_time > timeout:
                return False
            time.sleep(0.5)
        
        return self.working_count > 0
    
    # ==========================================================
    # 🔥 LEGACY ADAPTERS (DO NOT REMOVE - REQUIRED FOR SUITE)
    # ==========================================================
    
    @property
    def raw_proxies(self) -> List[str]:
        """Provides raw string array fallback for main_bot.py"""
        return [p["url"] for p in self.proxies]
        
    def parse_proxy_string(self, proxy_str: str) -> Optional[Dict[str, Any]]:
        """Compatibility alias for _parse_proxy"""
        return self._parse_proxy(proxy_str)
        
    def get_secured_proxy(self) -> Optional[Dict[str, Any]]:
        """Compatibility alias for adder.py & dmsender.py"""
        return self.get_proxy()
        
    def flag_proxy_failure(self, proxy: Dict[str, Any]) -> None:
        """Compatibility alias to punish dead proxies"""
        self.mark_failed(proxy)
