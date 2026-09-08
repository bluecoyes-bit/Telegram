#!/usr/bin/env python3
"""
Filename: config.py
"""

import os
import sys
import re
import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, asdict
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("SuiteConfig")

# ────────────────────────────────────────────────────────────────
# 1. HELPER: Environment-aware coercers with validation
# ────────────────────────────────────────────────────────────────

def _env_int(key: str, default: int, min_val: Optional[int] = None, max_val: Optional[int] = None) -> int:
    """Read integer from env with bounds clamping."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        val = int(raw.strip())
    except (ValueError, TypeError):
        logger.warning(f"⚠️ Env {key!r} = {raw!r} is not a valid int, using default {default}")
        return default
    if min_val is not None:
        val = max(val, min_val)
    if max_val is not None:
        val = min(val, max_val)
    return val


def _env_str(key: str, default: str) -> str:
    """Read string from env with trim."""
    raw = os.environ.get(key)
    return raw.strip() if raw else default


def _env_float(key: str, default: float) -> float:
    """Read float from env."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except (ValueError, TypeError):
        return default


def _env_bool(key: str, default: bool) -> bool:
    """Read boolean from env (true/1/yes = True)."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes", "y", "on")


def _validate_required(key: str, value: Any, display_name: str) -> None:
    """Log a fatal warning if a critical config value is missing/placeholder."""
    if not value or (isinstance(value, str) and len(value) < 4):
        logger.warning(
            f"⚠️ CRITICAL: {display_name} ({key}) appears incomplete or missing. "
            f"Set environment variable {key} or update CONFIG."
        )


# ────────────────────────────────────────────────────────────────
# 2. TYPED SUBCONFIG DATACLASSES FOR PERFORMANCE TUNING
# ────────────────────────────────────────────────────────────────

@dataclass
class PoolConfig:
    max_pool_size: int = field(default_factory=lambda: _env_int("MAX_POOL_SIZE", 50, 10, 500))
    max_pool_absolute: int = field(default_factory=lambda: _env_int("MAX_POOL_ABSOLUTE", 200, 20, 1000))
    status_bar_cache_ttl: int = field(default_factory=lambda: _env_int("STATUS_BAR_CACHE_TTL", 30, 5, 300))
    pool_cleanup_max_idle: int = field(default_factory=lambda: _env_int("POOL_CLEANUP_MAX_IDLE", 3600, 300, 14400))
    auth_state_ttl: int = field(default_factory=lambda: _env_int("AUTH_STATE_TTL", 300, 60, 900))
    client_timeout: float = field(default_factory=lambda: _env_float("CLIENT_TIMEOUT", 10.0))
    connect_timeout: float = field(default_factory=lambda: _env_float("CONNECT_TIMEOUT", 15.0))

@dataclass
class AuditorConfig:
    """Background session auditor tuning."""
    batch_size: int = field(default_factory=lambda: _env_int("AUDITOR_BATCH_SIZE", 10, 1, 100))
    batch_stagger: int = field(default_factory=lambda: _env_int("AUDITOR_BATCH_STAGGER", 15, 5, 120))
    cooldown_min: int = field(default_factory=lambda: _env_int("AUDITOR_COOLDOWN_MIN", 300, 60, 3600))
    cooldown_max: int = field(default_factory=lambda: _env_int("AUDITOR_COOLDOWN_MAX", 600, 120, 7200))
    recheck_minutes: int = field(default_factory=lambda: _env_int("AUDITOR_RECHECK_MINUTES", 720, 30, 2880))
    concurrency: int = field(default_factory=lambda: _env_int("AUDITOR_CONCURRENCY", 3, 1, 10))
    enabled: bool = field(default_factory=lambda: _env_bool("AUDITOR_ENABLED", True))

@dataclass
class AdderConfig:
    """Member adder rate-limit & burst protection."""
    max_workers: int = field(default_factory=lambda: _env_int("ADDER_MAX_WORKERS", 20, 1, 50))
    max_worker_sessions: int = field(default_factory=lambda: _env_int("ADDER_MAX_WORKER_SESSIONS", 10, 1, 30))
    human_add_interval: Tuple[int, int] = (8, 14)
    burst_add_limit: int = field(default_factory=lambda: _env_int("ADDER_BURST_ADD_LIMIT", 6, 1, 20))
    burst_cooldown: Tuple[int, int] = (30, 50)
    progress_interval: int = field(default_factory=lambda: _env_int("ADDER_PROGRESS_UPDATE_INTERVAL", 8, 2, 30))
    account_launch_delay: Tuple[int, int] = (8, 15)

@dataclass
class MongoConfig:
    """MongoDB connection & pool tuning."""
    min_pool_size: int = field(default_factory=lambda: _env_int("MONGO_MIN_POOL_SIZE", 5, 1, 50))
    max_pool_size: int = field(default_factory=lambda: _env_int("MONGO_MAX_POOL_SIZE", 100, 10, 500))
    max_idle_time_ms: int = field(default_factory=lambda: _env_int("MONGO_MAX_IDLE_MS", 300000, 60000, 600000))  # 5 min
    wait_queue_timeout_ms: int = field(default_factory=lambda: _env_int("MONGO_WAIT_QUEUE_MS", 10000, 1000, 60000))
    connect_timeout_ms: int = field(default_factory=lambda: _env_int("MONGO_CONNECT_TIMEOUT_MS", 10000, 2000, 30000))
    server_selection_timeout_ms: int = field(default_factory=lambda: _env_int("MONGO_SERVER_SEL_TIMEOUT_MS", 10000, 2000, 60000))
    retry_writes: bool = field(default_factory=lambda: _env_bool("MONGO_RETRY_WRITES", True))
    retry_reads: bool = field(default_factory=lambda: _env_bool("MONGO_RETRY_READS", True))
    compressors: str = field(default_factory=lambda: _env_str("MONGO_COMPRESSORS", "zlib"))
    zlib_compression_level: int = field(default_factory=lambda: _env_int("MONGO_ZLIB_LEVEL", 5, 1, 9))


# ────────────────────────────────────────────────────────────────
# 3. MASTER CONFIG DICT (backward-compatible with existing code)
# ────────────────────────────────────────────────────────────────

# ── CORE CREDENTIALS (set via env vars for security) ──
# Hardcoded fallbacks are provided ONLY for local dev; set env vars in production.
CONFIG: Dict[str, Any] = {
    # ── Core API Credentials (NO HARDCODED DEFAULTS - Must set env vars) ──
    "API_ID": _env_int("API_ID", 0, min_val=0),
    "API_HASH": _env_str("API_HASH", ""),
    "BOT_TOKEN": _env_str("BOT_TOKEN", ""),

    # ── Admin ──
    "ADMIN_ID": _env_str("ADMIN_ID", ""),

    # ── Web API security ──
    # Token required to call any /console/api/* route (Authorization: Bearer or
    # X-API-Token header). Unset -> requests are rejected with 503 until the
    # operator configures it. NEVER leave the REST console unauthenticated.
    "WEB_API_TOKEN": _env_str("WEB_API_TOKEN", ""),

    # ── Worker Identity ──
    "WORKER_NODE_ID": _env_str("WORKER_NODE_ID", "worker_01"),

    # ── Batch Sequencing ──
    "BATCH_SEQUENCE_START": _env_int("BATCH_START", 0, 0),
    "BATCH_SEQUENCE_END": _env_int("BATCH_END", 250, 1),

    # ── Timeouts & Retries ──
    "CONNECTION_TIMEOUT": _env_float("CONNECTION_TIMEOUT", 15.0),
    "API_REQUEST_RETRIES": _env_int("API_RETRIES", 3, 1, 10),

    # ── Anti-Ban / Rate-Limiting ──
    "ACCOUNT_LAUNCH_DELAY": (8, 15),
    "HUMAN_ADD_INTERVAL": (25, 45),
    "BURST_ADD_LIMIT": _env_int("BURST_ADD_LIMIT", 5, 1, 20),
    "BURST_COOLDOWN_TIME": (60, 120),
    "LOOP_KEEP_ALIVE": (30, 45),

    "MAX_POOL_SIZE": _env_int("MAX_POOL_SIZE", 50, 10, 500),
    "MAX_POOL_ABSOLUTE": _env_int("MAX_POOL_ABSOLUTE", 200, 20, 1000),
    "STATUS_BAR_CACHE_TTL": _env_int("STATUS_BAR_CACHE_TTL", 30, 5, 300),
    "POOL_CLEANUP_MAX_IDLE": _env_int("POOL_CLEANUP_MAX_IDLE", 3600, 300, 14400),
    "AUTH_STATE_TTL": _env_int("AUTH_STATE_TTL", 300, 60, 900),
    "CLIENT_TIMEOUT": _env_float("CLIENT_TIMEOUT", 10.0),

    # ── Auditor ──
    "AUDITOR_BATCH_SIZE": _env_int("AUDITOR_BATCH_SIZE", 10, 1, 100),
    "AUDITOR_BATCH_STAGGER": _env_int("AUDITOR_BATCH_STAGGER", 60, 35, 220),
    # Pass gap between auditor sweeps (per-account recheck cadence is
    # AUDITOR_RECHECK_MINUTES below). The 3-5 min proxy rest window lives in
    # resource_manager and paces individual checks on top of this.
    "AUDITOR_COOLDOWN_MIN": _env_int("AUDITOR_COOLDOWN_MIN", 300, 60, 3600),
    "AUDITOR_COOLDOWN_MAX": _env_int("AUDITOR_COOLDOWN_MAX", 600, 120, 7200),
    "AUDITOR_RECHECK_MINUTES": _env_int("AUDITOR_RECHECK_MINUTES", 720, 30, 2880),
    "AUDITOR_CONCURRENCY": _env_int("AUDITOR_CONCURRENCY", 3, 1, 10),
    "AUDITOR_ENABLED": _env_bool("AUDITOR_ENABLED", True),

    # ── Adder ──
    "ADDER_MAX_WORKERS": _env_int("ADDER_MAX_WORKERS", 20, 1, 50),
    "ADDER_MAX_WORKER_SESSIONS": _env_int("ADDER_MAX_WORKER_SESSIONS", 10, 1, 30),
    "ADDER_HUMAN_ADD_INTERVAL": (8, 14),
    "ADDER_BURST_ADD_LIMIT": _env_int("ADDER_BURST_ADD_LIMIT", 6, 1, 20),
    "ADDER_BURST_COOLDOWN_TIME": (30, 50),
    "ADDER_PROGRESS_UPDATE_INTERVAL": _env_int("ADDER_PROGRESS_UPDATE_INTERVAL", 8, 2, 30),

    # ── MongoDB ──
    "MONGO_MIN_POOL_SIZE": _env_int("MONGO_MIN_POOL_SIZE", 5, 1, 50),
    "MONGO_MAX_POOL_SIZE": _env_int("MONGO_MAX_POOL_SIZE", 100, 10, 500),
    "MONGO_MAX_IDLE_TIME_MS": _env_int("MONGO_MAX_IDLE_MS", 300000, 60000, 600000),
    "MONGO_WAIT_QUEUE_TIMEOUT_MS": _env_int("MONGO_WAIT_QUEUE_MS", 10000, 1000, 60000),
    "MONGO_CONNECT_TIMEOUT_MS": _env_int("MONGO_CONNECT_TIMEOUT_MS", 10000, 2000, 30000),
    "MONGO_SERVER_SELECTION_TIMEOUT_MS": _env_int("MONGO_SERVER_SEL_TIMEOUT_MS", 10000, 2000, 60000),
    "MONGO_RETRY_WRITES": _env_bool("MONGO_RETRY_WRITES", True),
    "MONGO_RETRY_READS": _env_bool("MONGO_RETRY_READS", True),
    "MONGO_COMPRESSORS": _env_str("MONGO_COMPRESSORS", "zstd,zlib,snappy"),
    "MONGO_ZLIB_COMPRESSION_LEVEL": _env_int("MONGO_ZLIB_LEVEL", 5, 1, 9),

    # ── Feature Flags ──
    "ENABLE_AUTO_RECOVERY": _env_bool("ENABLE_AUTO_RECOVERY", True),

    # ── Auto-Recovery / Health Scan ──
    # Human-paced sweep of failed/muted accounts: initial delay before the first
    # sweep, interval between sweeps, and the 3-6 minute per-account delay
    # (micro-jitter is added in code).
    "RECOVERY_INITIAL_DELAY": _env_int("RECOVERY_INITIAL_DELAY", 600, 60, 3600),
    "RECOVERY_INTERVAL": _env_int("RECOVERY_INTERVAL", 21600, 3600, 86400),
    "RECOVERY_ACCOUNT_DELAY": (180, 360),
    "HEALTH_SCAN_CONCURRENCY": _env_int("HEALTH_SCAN_CONCURRENCY", 3, 1, 10),
    "ENABLE_CONTACT_SCRAPER": _env_bool("ENABLE_CONTACT_SCRAPER", True),
    "ENABLE_VOICE_CHAT": _env_bool("ENABLE_VOICE_CHAT", True),
    "ENABLE_HEALTH_CHECK": _env_bool("ENABLE_HEALTH_CHECK", True),

    # ── Logging ──
    "LOG_LEVEL": _env_str("LOG_LEVEL", "INFO").upper(),
    "TELETHON_LOG_LEVEL": _env_str("TELETHON_LOG_LEVEL", "WARNING").upper(),
    "SUITE_LOG_FORMAT": _env_str("SUITE_LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
}

# ── Runtime validation of critical values ──
if not CONFIG["API_ID"]:
    logger.warning("⚠️ CRITICAL: API_ID is not set. Set environment variable API_ID.")
if not CONFIG["API_HASH"]:
    logger.warning("⚠️ CRITICAL: API_HASH is not set. Set environment variable API_HASH.")
if not CONFIG["BOT_TOKEN"]:
    logger.warning("⚠️ CRITICAL: BOT_TOKEN is not set. Bot will not start without a valid token.")
if not CONFIG["ADMIN_ID"]:
    logger.warning("⚠️ CRITICAL: ADMIN_ID is not set. All users will be treated as admin (insecure for production).")


# ────────────────────────────────────────────────────────────────
# 4. TYPED SUBCONFIG INSTANCES (for use in performance-critical paths)
# ────────────────────────────────────────────────────────────────

POOL_CFG = PoolConfig()
AUDITOR_CFG = AuditorConfig()
ADDER_CFG = AdderConfig()
MONGO_CFG = MongoConfig()


# ────────────────────────────────────────────────────────────────
# 5. MONGODB SETTINGS (single-DB ecosystem, 5 collections)
# ────────────────────────────────────────────────────────────────

# Build Mongo URI from env with secure fallback (NO HARDCODED CREDENTIALS)
# Check both standard names and .env-compatible names
_MONGO_USER = os.environ.get("MONGO_USER") or os.environ.get("_MONGO_USER", "")
_MONGO_PASS = os.environ.get("MONGO_PASS") or os.environ.get("_MONGO_PASS", "")
_MONGO_HOST = os.environ.get("MONGO_HOST") or os.environ.get("_MONGO_HOST", "")
_MONGO_OPTIONS = _env_str("MONGO_OPTIONS", "retryWrites=true&w=majority&appName=Cluster0")

_MONGO_URI_BUILT = os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URI", "")

MONGODB_SETTINGS = {
    "MONGO_URI": _MONGO_URI_BUILT,

    # ── 5-Collection Ecosystem (Single-DB) ──
    "SOURCE_DB_NAME": _env_str("MONGO_DB_NAME", "telegram_bot_db"),
    "SOURCE_ACCOUNTS_COLLECTION": "source_accounts",
    "OTP_LOGS_COLLECTION": "otps",
    "SESSION_BACKUP_COLLECTION": "session_backups",
    "SCRAPED_MEMBERS_COLLECTION": "scraped_data",
    "PROCESSED_MEMBERS_COLLECTION": "processed_history",
    "TELEMETRY_LOGS_COLLECTION": "system_telemetry",

    # ── SSL/TLS (use certifi system CA bundle) ──
    "MONGO_KWARGS": {
        "tls": _env_bool("MONGO_TLS", True),
        "tlsCAFile": os.environ.get("SSL_CERT_FILE", ""),
        "tlsAllowInvalidCertificates": _env_bool("MONGO_TLS_INSECURE", False),
        "serverSelectionTimeoutMS": MONGO_CFG.server_selection_timeout_ms,
        "connectTimeoutMS": MONGO_CFG.connect_timeout_ms,
        "maxPoolSize": MONGO_CFG.max_pool_size,
        "minPoolSize": MONGO_CFG.min_pool_size,
        "maxIdleTimeMS": MONGO_CFG.max_idle_time_ms,
        "waitQueueTimeoutMS": MONGO_CFG.wait_queue_timeout_ms,
        "retryWrites": MONGO_CFG.retry_writes,
        "retryReads": MONGO_CFG.retry_reads,
        "compressors": MONGO_CFG.compressors,
        "zlibCompressionLevel": MONGO_CFG.zlib_compression_level,
        "appName": "TelegramSuite",
    },
}

# ── If no explicit cert file, fall back to certifi ──
if not MONGODB_SETTINGS["MONGO_KWARGS"]["tlsCAFile"]:
    import certifi
    MONGODB_SETTINGS["MONGO_KWARGS"]["tlsCAFile"] = certifi.where()


# ────────────────────────────────────────────────────────────────
# 6. VALIDATION & STARTUP SANITY CHECK
# ────────────────────────────────────────────────────────────────

def validate_config() -> List[str]:
    """
    Run pre-flight checks on all critical config values.
    Returns a list of warning messages (empty = all good).
    """
    warnings: List[str] = []

    # API credentials - FATAL if missing
    if CONFIG["API_ID"] == 0:
        warnings.append("FATAL: API_ID is not set. Set environment variable API_ID.")
    if not CONFIG["API_HASH"]:
        warnings.append("FATAL: API_HASH is not set. Set environment variable API_HASH.")
    if not CONFIG["BOT_TOKEN"]:
        warnings.append("FATAL: BOT_TOKEN is not set. Set environment variable BOT_TOKEN.")
    if not CONFIG["ADMIN_ID"]:
        warnings.append("FATAL: ADMIN_ID is not set. Set environment variable ADMIN_ID.")

    # MongoDB - FATAL if missing
    mongo_uri = MONGODB_SETTINGS.get("MONGO_URI", "")
    if not mongo_uri:
        # Try building from components
        user = _MONGO_USER
        pwd = _MONGO_PASS
        host = _MONGO_HOST
        if user and pwd and host:
            mongo_uri = f"mongodb+srv://{user}:{pwd}@{host}/?{_MONGO_OPTIONS}"
            MONGODB_SETTINGS["MONGO_URI"] = mongo_uri
        else:
            warnings.append("FATAL: MONGO_URI is not set. Set environment variable MONGO_URI or MONGO_USER/MONGO_PASS/MONGO_HOST.")

    # Pool sizing sanity
    if CONFIG["MAX_POOL_SIZE"] > CONFIG["MAX_POOL_ABSOLUTE"]:
        warnings.append(f"MAX_POOL_SIZE ({CONFIG['MAX_POOL_SIZE']}) > MAX_POOL_ABSOLUTE ({CONFIG['MAX_POOL_ABSOLUTE']}). Clamping will occur.")

    # Batch sequencing
    if CONFIG["BATCH_SEQUENCE_START"] >= CONFIG["BATCH_SEQUENCE_END"]:
        warnings.append(f"BATCH_SEQUENCE_START ({CONFIG['BATCH_SEQUENCE_START']}) >= BATCH_SEQUENCE_END ({CONFIG['BATCH_SEQUENCE_END']}). No accounts will be processed.")

    return warnings


def log_config_summary() -> None:
    """Log a one-line summary of key operational parameters at startup."""
    summary = (
        f"⚙️ Config Summary: "
        f"Worker={CONFIG['WORKER_NODE_ID']}, "
        f"API_ID={CONFIG['API_ID']}, "
        f"Batch={CONFIG['BATCH_SEQUENCE_START']}-{CONFIG['BATCH_SEQUENCE_END']}, "
        f"Pool={CONFIG['MAX_POOL_SIZE']}/{CONFIG['MAX_POOL_ABSOLUTE']}, "
        f"Auditor={CONFIG['AUDITOR_BATCH_SIZE']}x{CONFIG['AUDITOR_BATCH_STAGGER']}s, "
        f"MongoPool={MONGO_CFG.max_pool_size}, "
        f"AdderWorkers={CONFIG['ADDER_MAX_WORKERS']}"
    )
    logger.info(summary)

    # Log warnings if any
    for w in validate_config():
        logger.warning(f"⚠️ {w}")


# ────────────────────────────────────────────────────────────────
# 7. DEVICE PROFILES (unchanged — already robust and diverse)
# ────────────────────────────────────────────────────────────────

DEVICE_PROFILES = [
    # 💻 WINDOWS ENTERPRISE WORKSTATION ARRAYS
    {"device_model": "Dell OptiPlex 7010", "system_version": "Windows 11 Pro 23H2", "app_version": "5.1.0"},
    {"device_model": "HP EliteDesk 800 G9", "system_version": "Windows 11 Enterprise", "app_version": "5.0.4"},
    {"device_model": "Lenovo ThinkCentre M90q", "system_version": "Windows 11 Pro", "app_version": "5.1.2"},
    {"device_model": "Microsoft Surface Laptop 6", "system_version": "Windows 11 Home", "app_version": "5.0.0"},
    {"device_model": "ASUS ExpertBook B9", "system_version": "Windows 11 Pro", "app_version": "5.1.1"},
    {"device_model": "Acer TravelMate P6", "system_version": "Windows 11 Pro", "app_version": "5.0.8"},
    {"device_model": "MSI Prestige 16", "system_version": "Windows 11 Pro", "app_version": "5.1.3"},
    {"device_model": "Dell Latitude 7440", "system_version": "Windows 11 Enterprise", "app_version": "5.1.4"},
    {"device_model": "HP ZBook Fury", "system_version": "Windows 11 Workstation", "app_version": "5.1.5"},
    {"device_model": "Lenovo ThinkPad X1 Carbon Gen 12", "system_version": "Windows 11 Pro", "app_version": "5.1.2"},

    # 🍏 APPLE macOS SEQUOIA & SONOMA MATRIX CONTROLLERS
    {"device_model": "MacBook Pro M3 Max", "system_version": "macOS 15.0 Sequoia", "app_version": "5.2.1"},
    {"device_model": "MacBook Air M2", "system_version": "macOS 14.5 Sonoma", "app_version": "5.1.0"},
    {"device_model": "Mac Studio Core", "system_version": "macOS 14.6.1 Sonoma", "app_version": "5.0.2"},
    {"device_model": "iMac 24-inch", "system_version": "macOS 15.1 Sequoia", "app_version": "5.2.4"},

    # 🐧 LINUX DEBIAN / REDHAT PRODUCTION DEPLOYMENTS
    {"device_model": "Ubuntu Desktop", "system_version": "Linux 24.04 LTS", "app_version": "5.0.0"},
    {"device_model": "Fedora Workstation", "system_version": "Linux Kernel 6.8", "app_version": "4.16.5"},
    {"device_model": "RedHat Enterprise", "system_version": "RHEL 9.4 Node", "app_version": "4.15.2"},
    {"device_model": "Debian GNU/Linux", "system_version": "Debian 12 Bookworm", "app_version": "5.1.1"},

    # 🤖 ANDROID EXTENDED BRAND MATRIX
    {"device_model": "Samsung Galaxy S24 Ultra", "system_version": "Android 14", "app_version": "11.2.4"},
    {"device_model": "Samsung Galaxy Z Fold 6", "system_version": "Android 14", "app_version": "11.2.1"},
    {"device_model": "Google Pixel 9 Pro XL", "system_version": "Android 15", "app_version": "11.3.0"},
    {"device_model": "OnePlus 12", "system_version": "Android 14", "app_version": "11.1.8"},

    # 🔴 REDMI & XIAOMI SERIES
    {"device_model": "Redmi Note 13 Pro+ 5G", "system_version": "Android 14 (HyperOS)", "app_version": "11.0.6"},
    {"device_model": "Redmi Note 12 Pro", "system_version": "Android 13", "app_version": "10.5.2"},
    {"device_model": "Redmi 13C", "system_version": "Android 14", "app_version": "11.0.1"},
    {"device_model": "Xiaomi 14 Ultra", "system_version": "Android 14", "app_version": "11.0.5"},

    # 🟡 POCO PERFORMANCE ENGINES
    {"device_model": "Poco F6 Pro", "system_version": "Android 14", "app_version": "11.1.2"},
    {"device_model": "Poco X6 Pro 5G", "system_version": "Android 14", "app_version": "11.0.9"},
    {"device_model": "Poco M6 Pro", "system_version": "Android 13", "app_version": "10.4.5"},

    # 🟠 REALME DYNAMIC LAYERS
    {"device_model": "Realme GT 6", "system_version": "Android 14", "app_version": "11.1.5"},
    {"device_model": "Realme 12 Pro+ 5G", "system_version": "Android 14", "app_version": "11.0.8"},
    {"device_model": "Realme Narzo 70 Pro", "system_version": "Android 14", "app_version": "11.0.3"},
    {"device_model": "Realme C67", "system_version": "Android 14", "app_version": "11.0.1"},

    # 🔵 VIVO FLAGSHIPS & SLOTS
    {"device_model": "Vivo X100 Pro", "system_version": "Android 14", "app_version": "11.1.8"},
    {"device_model": "Vivo V30 Pro", "system_version": "Android 14", "app_version": "11.0.7"},
    {"device_model": "Vivo T3 5G", "system_version": "Android 14", "app_version": "11.0.4"},
    {"device_model": "Vivo Y200 Pro", "system_version": "Android 14", "app_version": "11.0.2"},

    # 🟢 OPPO SMARTPHONE MODULES
    {"device_model": "Oppo Find X7 Ultra", "system_version": "Android 14", "app_version": "11.1.6"},
    {"device_model": "Oppo Reno 12 Pro", "system_version": "Android 14", "app_version": "11.1.1"},
    {"device_model": "Oppo F27 Pro+ 5G", "system_version": "Android 14", "app_version": "11.0.5"},
    {"device_model": "Oppo A3 Pro", "system_version": "Android 14", "app_version": "11.0.2"},

    # 🛡️ HONOR SECURITY PLATFORMS
    {"device_model": "Honor Magic 6 Pro", "system_version": "Android 14", "app_version": "11.1.7"},
    {"device_model": "Honor 200 Pro", "system_version": "Android 14", "app_version": "11.0.9"},
    {"device_model": "Honor X9b", "system_version": "Android 13", "app_version": "10.6.1"},

    # ⚡ INFINIX HIGH-SPEED NODES
    {"device_model": "Infinix GT 20 Pro", "system_version": "Android 14", "app_version": "11.0.8"},
    {"device_model": "Infinix Note 40 Pro+ 5G", "system_version": "Android 14", "app_version": "11.0.5"},
    {"device_model": "Infinix Zero 30", "system_version": "Android 13", "app_version": "10.5.1"},
    {"device_model": "Infinix Smart 8", "system_version": "Android 13", "app_version": "10.2.0"},

    # 🇮🇳 MICROMAX LEGACY & RECENT ENTRIES
    {"device_model": "Micromax In Note 2", "system_version": "Android 12", "app_version": "9.8.4"},
    {"device_model": "Micromax In 2c", "system_version": "Android 11", "app_version": "9.2.1"},
    {"device_model": "Micromax In 1b", "system_version": "Android 10", "app_version": "8.9.5"},

    # 🍏 APPLE iOS HIGH-FIDELITY MOBILE PLATFORMS
    {"device_model": "iPhone 16 Pro Max", "system_version": "iOS 18.0", "app_version": "11.4.0"},
    {"device_model": "iPhone 16 Pro", "system_version": "iOS 18.0", "app_version": "11.4.0"},
    {"device_model": "iPhone 15 Pro Max", "system_version": "iOS 17.5.1", "app_version": "11.3.1"},
    {"device_model": "iPhone 15 Pro", "system_version": "iOS 17.5", "app_version": "11.3.0"},
    {"device_model": "iPhone 15", "system_version": "iOS 17.5", "app_version": "11.3.0"},
    {"device_model": "iPhone 14 Plus", "system_version": "iOS 17.4", "app_version": "11.2.0"},
    {"device_model": "iPhone 14 Pro", "system_version": "iOS 17.4", "app_version": "11.2.2"},
    {"device_model": "iPad Pro M4", "system_version": "iPadOS 17.6", "app_version": "11.2.5"},
    {"device_model": "iPad Air M2", "system_version": "iPadOS 17.5", "app_version": "11.2.4"},
]


# ────────────────────────────────────────────────────────────────
# 8. AUTO-RUN VALIDATION ON IMPORT
# ────────────────────────────────────────────────────────────────

log_config_summary()