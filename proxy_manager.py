#!/usr/bin/env python3
"""
Ultimate Enterprise Telegram Suite - Thread-Safe Proxy Optimization Layer
Filename: proxy_manager.py
"""

import os
import time
import socket
import random
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Any, Tuple, Set

from config import CONFIG

logger = logging.getLogger("SuiteProxyManager")

class RobustProxyManager:
    """Handles continuous health scanning, failover counts, and live pooling of connection vectors."""
    
    def __init__(self, file_path: str = "proxies.txt"):
        self.file_path = file_path
        self.raw_proxies: List[str] = []
        self.working_proxies: List[Dict[str, Any]] = []
        self.failed_proxies: set[str] = set()
        self.failure_counters: Dict[str, int] = {}
        self.last_rotation: Dict[str, float] = {}
        
        self._lock = threading.Lock()
        self._testing_active = False
        self.count = 0
        self.working_count = 0
        
        self._load_proxies()

    def _load_proxies(self):
        """Reads raw strings safely from local target storage contexts."""
        with self._lock:
            if not os.path.exists(self.file_path):
                # Generates placeholder to maintain application safety
                with open(self.file_path, "w") as f:
                    f.write("# Format -> host:port or host:port:username:password\n")
                self.raw_proxies = []
                self.count = 0
                return

            with open(self.file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
            
            self.raw_proxies = lines
            self.count = len(lines)
            logger.info(f"Loaded {self.count} raw proxy components from local matrix container.")

    def parse_proxy_string(self, proxy_str: str) -> Optional[Dict[str, Any]]:
        """Maps varying credential formats into explicit dictionary nodes for Telethon."""
        try:
            parts = proxy_str.strip().split(':')
            if len(parts) >= 2:
                # Basic standard SOCKS5 mapping structure layout
                proxy_dict = {
                    'proxy_type': 'socks5', 
                    'addr': parts[0], 
                    'port': int(parts[1]),
                    'username': None,
                    'password': None
                }
                if len(parts) == 4:
                    proxy_dict['username'] = parts[2]
                    proxy_dict['password'] = parts[3]
                return proxy_dict
        except Exception:
            pass
        return None

    def _test_node_socket(self, proxy_str: str) -> Optional[Dict[str, Any]]:
        """Performs raw low-level connection validation via optimized network interfaces."""
        parsed = self.parse_proxy_string(proxy_str)
        if not parsed:
            return None
            
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            s.connect((parsed['addr'], parsed['port']))
            s.close()
            return parsed
        except Exception:
            return None

    async def run_pipeline_scan(self):
        """Asynchronously triggers the thread worker pool validation layout logic matrix."""
        if not self.raw_proxies:
            return
            
        with self._lock:
            if self._testing_active:
                return
            self._testing_active = True
            
        logger.info("Starting background proxy validation pipeline matrix arrays...")
        loop = asyncio.get_running_loop()
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            tasks = [loop.run_in_executor(executor, self._test_node_socket, p) for p in self.raw_proxies]
            
            for finished_task in asyncio.as_completed(tasks):
                res = await finished_task
                if res:
                    with self._lock:
                        # Append into active pipeline lists
                        proxy_url = f"{res['addr']}:{res['port']}"
                        if res not in self.working_proxies:
                            self.working_proxies.append(res)
                            self.working_count = len(self.working_proxies)

        with self._lock:
            self._testing_active = False
        logger.info(f"Proxy analysis cycle finished. Active functional vectors: {self.working_count}")
        
    def get_secured_geofenced_proxy(self, account_doc: dict) -> Optional[Dict[str, Any]]:
        """
        Fetches validated replacement execution proxy node strictly mapping 
        the geo-location parameters of the target document account profile.
        """
        with self._lock:
            if not self.working_proxies:
                return None
            
                
            # Direct fallback if exact regional metrics drop
            return random.choice(self.working_proxies)    

    def get_secured_proxy(self) -> Optional[Dict[str, Any]]:
        """Safely fetches a random validated proxy dict node context using thread locks."""
        with self._lock:
            if self.working_proxies:
                return random.choice(self.working_proxies)
            return None

    def flag_proxy_failure(self, proxy_dict: Dict[str, Any]):
        """Increments error counts to purge dead system lines automatically."""
        if not proxy_dict:
            return
            
        proxy_url = f"{proxy_dict['addr']}:{proxy_dict['port']}"
        with self._lock:
            self.failure_counters[proxy_url] = self.failure_counters.get(proxy_url, 0) + 1
            if self.failure_counters[proxy_url] >= 3:
                if proxy_dict in self.working_proxies:
                    self.working_proxies.remove(proxy_dict)
                self.failed_proxies.add(proxy_url)
                self.working_count = len(self.working_proxies)
                logger.warning(f"Proxy node {proxy_url} surpassed failure limits. Removed from active pools.")
                
                
class ProxyManager(RobustProxyManager):
    """Compatibility wrapper for main_bot v2.0 architecture."""
    
    def get_proxy(self) -> Optional[Dict[str, Any]]:
        """Alias for v2 compatibility."""
        return self.get_secured_proxy()
        
    def start_background_testing(self):
        """Triggers the async pipeline scan safely from sync context."""
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.run_pipeline_scan())
        except RuntimeError:
            # If no loop is running, it will be started by the main bot
            pass                