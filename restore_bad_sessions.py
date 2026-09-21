#!/usr/bin/env python3
"""Restore accounts wrongly flagged as 'bad session' (Not a valid string).

Standalone. Does not import or modify main_bot.py / database.py.

What actually happened
----------------------
source_accounts.session / session_string were overwritten with a 100-char
auditor *reason* string, e.g.:

    Account terminated: you're banned from sending messages in supergroups/...

That is ChatWriteForbidden / channel-ban text, not a Telethon key. Health Scan
then raised ValueError('Not a valid string') and tagged last_error=
invalid_session_string. The real keys are still in session_backups
(status_snapshot=active on 12 Sep). Duplicate backup rows share one key;
this script keeps unique valid strings only (newest first).

Dead (Telegram said so)
-----------------------
- Session unauthorized / cannot fetch profile
- Auth key used under two different IP addresses simultaneously

Active
------
- get_me() / profile fetch succeeds (channel-send bans are NOT dead)

Usage
-----
  python restore_bad_sessions.py                  # dry-run report only
  python restore_bad_sessions.py --apply          # write best unique valid string
  python restore_bad_sessions.py --apply --live   # Telegram get_me → active/dead
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

from pymongo import MongoClient
from telethon import TelegramClient
from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)
from telethon.sessions import StringSession

from config import CONFIG, MONGODB_SETTINGS, MONGO_CFG, validate_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - restore_bad_sessions - %(levelname)s - %(message)s",
)
log = logging.getLogger("restore_bad_sessions")

INVALID_MARKERS = (
    "invalid_session_string",
    "not a valid string",
    "invalid session",
)
DEAD_TEXT_MARKERS = (
    "session unauthorized",
    "unauthorized",
    "unable to fetch profile",
    "authorization key",
    "two different ip",
    "used under two",
    "authkeyunregistered",
    "sessionrevoked",
    "authkeyduplicated",
)
REASON_PREFIXES = (
    "account terminated",
    "account terminated:",
    "you're banned from sending",
    "the channel specified is private",
)


def _digits(phone: Any) -> str:
    return "".join(c for c in str(phone or "") if c.isdigit())


def _looks_flagged(doc: dict) -> bool:
    blob = " ".join(
        str(doc.get(k) or "")
        for k in ("last_error", "revocation_reason", "status")
    ).lower()
    if any(m in blob for m in INVALID_MARKERS):
        return True
    live = _clean_session(doc.get("session_string") or doc.get("session"))
    return _looks_like_reason_text(live)


def _looks_like_reason_text(text: str) -> bool:
    low = text.lower().strip()
    if any(low.startswith(p) for p in REASON_PREFIXES):
        return True
    if " " in text[:24] and len(text) <= 120:
        return True
    return False


def _clean_session(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    text = str(raw).strip()
    if not text or text.lower() in ("none", "null", "nil"):
        return ""
    if (text.startswith('"') and text.endswith('"')) or (
        text.startswith("'") and text.endswith("'")
    ):
        text = text[1:-1].strip()
    if text.startswith("b'") and text.endswith("'"):
        text = text[2:-1].strip()
    if text.startswith('b"') and text.endswith('"'):
        text = text[2:-1].strip()
    return text


def _is_telethon_string(session_str: str) -> bool:
    if not session_str or len(session_str) < 80:
        return False
    if _looks_like_reason_text(session_str):
        return False
    try:
        StringSession(session_str)
        return True
    except Exception:
        return False


def _fp(session_str: str) -> str:
    return hashlib.sha256(session_str.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _mongo():
    validate_config()
    uri = MONGODB_SETTINGS.get("MONGO_URI") or ""
    if not uri:
        raise SystemExit("MONGO_URI is not set.")
    kwargs = dict(MONGODB_SETTINGS.get("MONGO_KWARGS") or {})
    kwargs.update({
        "maxPoolSize": 10,
        "minPoolSize": 0,
        "serverSelectionTimeoutMS": MONGO_CFG.server_selection_timeout_ms,
        "connectTimeoutMS": MONGO_CFG.connect_timeout_ms,
        "socketTimeoutMS": 20000,
    })
    client = MongoClient(uri, **kwargs)
    client.admin.command("ping")
    db = client[MONGODB_SETTINGS["SOURCE_DB_NAME"]]
    return client, db[MONGODB_SETTINGS["SOURCE_ACCOUNTS_COLLECTION"]], db[
        MONGODB_SETTINGS["SESSION_BACKUP_COLLECTION"]
    ]


def _unique_valid(account: dict, backups: list[dict]) -> list[tuple[str, str, dict]]:
    """Newest unique Telethon-valid strings. Duplicates of the same key are skipped."""
    ordered: list[tuple[str, str, dict]] = []
    seen: set[str] = set()

    def add(source: str, raw: Any, meta: Optional[dict] = None) -> None:
        cleaned = _clean_session(raw)
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        if not _is_telethon_string(cleaned):
            return
        ordered.append((source, cleaned, meta or {}))

    dated = sorted(
        backups,
        key=lambda b: b.get("backup_created_at") or 0,
        reverse=True,
    )
    for i, b in enumerate(dated):
        add(
            f"backup[{i}]",
            b.get("session_snapshot") or b.get("session") or b.get("session_string"),
            b,
        )
    add("account.session_string", account.get("session_string"), account)
    add("account.session", account.get("session"), account)
    return ordered


def _decodo_proxy(sticky: str) -> Optional[dict]:
    user = os.environ.get("DECODO_USERNAME") or os.environ.get("PROXY_USERNAME") or ""
    pwd = os.environ.get("DECODO_PASSWORD") or os.environ.get("PROXY_PASSWORD") or ""
    host = os.environ.get("DECODO_HOST", "dc.decodo.com")
    port = int(os.environ.get("DECODO_PORT", "10001"))
    if not user or not pwd:
        return None
    tag = "".join(c for c in sticky if c.isalnum())[-12:] or "restore"
    return {
        "proxy_type": "socks5",
        "addr": host,
        "port": port,
        "username": f"user-{user}-session-{tag}",
        "password": pwd,
        "rdns": True,
    }


async def _live_verdict(
    session_str: str, api_id: int, api_hash: str, device: dict, sticky: str,
) -> str:
    """active | dead | skip — profile fetch is the only 'active' signal."""
    proxy = _decodo_proxy(sticky)
    client = TelegramClient(
        StringSession(session_str),
        api_id=api_id,
        api_hash=api_hash,
        device_model=device.get("device_model", "PC 64bit"),
        system_version=device.get("system_version", "Windows 11"),
        app_version=device.get("app_version", "4.8.4"),
        proxy=proxy,
        timeout=12.0,
        connection_retries=1,
        request_retries=1,
        receive_updates=False,
    )
    try:
        await asyncio.wait_for(client.connect(), timeout=20.0)
        me = await asyncio.wait_for(client.get_me(), timeout=12.0)
        if me:
            return "active"
        return "dead"
    except (AuthKeyUnregisteredError, SessionRevokedError,
            UserDeactivatedError, UserDeactivatedBanError, AuthKeyDuplicatedError):
        return "dead"
    except Exception as exc:
        msg = str(exc).lower()
        if any(m in msg for m in DEAD_TEXT_MARKERS):
            return "dead"
        if "not a valid string" in msg:
            return "skip"
        log.info(f"   live-check skip ({type(exc).__name__}: {exc})")
        return "skip"
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def _device_of(account: dict) -> dict:
    meta = account.get("device_metadata") or {}
    return {
        "device_model": meta.get("device_model") or account.get("device_model") or "PC 64bit",
        "system_version": meta.get("system_version") or account.get("system_version") or "Windows 11",
        "app_version": meta.get("app_version") or account.get("app_version") or "4.8.4",
    }


def _api_of(account: dict, uniques: list[tuple[str, str, dict]]) -> tuple[int, str]:
    api_id = account.get("api_id")
    api_hash = account.get("api_hash")
    for _, _, meta in uniques:
        api_id = api_id or meta.get("api_id")
        api_hash = api_hash or meta.get("api_hash")
    return int(api_id or CONFIG.get("API_ID") or 0), str(api_hash or CONFIG.get("API_HASH") or "")


def restore_docs(src, backups_col) -> list[dict]:
    flagged = [doc for doc in src.find({}) if _looks_flagged(doc)]
    rows = []
    for acc in flagged:
        phone = _digits(acc.get("phone"))
        variants = [phone, acc.get("phone"), f"+{phone}"]
        bdocs = list(backups_col.find({"phone": {"$in": variants}}))
        if not bdocs:
            bdocs = list(backups_col.find({"phone": {"$regex": f"{phone}$"}}))
        uniques = _unique_valid(acc, bdocs)
        pick = (uniques[0][0], uniques[0][1]) if uniques else None
        api_id, api_hash = _api_of(acc, uniques)
        rows.append({
            "phone": phone,
            "status": str(acc.get("status") or ""),
            "last_error": str(acc.get("last_error") or "")[:80],
            "backup_docs": len(bdocs),
            "unique_valid": len(uniques),
            "uniques": uniques,
            "pick": pick,
            "account": acc,
            "api_id": api_id,
            "api_hash": api_hash,
            "device": _device_of(acc),
        })
    return rows


def _write_account(src, row: dict, session_str: str, verdict: str) -> None:
    now = datetime.now(timezone.utc)
    phone_key = row["account"].get("phone")
    payload: dict[str, Any] = {
        "session": session_str,
        "session_string": session_str,
        "last_updated": now,
        "last_checked_time": now,
        "last_error": "",
        "revocation_reason": "",
        "spam_until": None,
    }
    if verdict == "active":
        payload["status"] = "active"
    elif verdict == "dead":
        payload["status"] = "revoked"
        payload["last_error"] = "telegram_session_dead"
        payload["revocation_reason"] = (
            "Session unauthorized / auth-key duplicated / unable to fetch profile"
        )
    else:
        payload["status"] = "failed"
    src.update_one({"phone": phone_key}, {"$set": payload})


async def maybe_live(rows: list[dict], apply: bool, live: bool, src, concurrency: int) -> dict:
    sem = asyncio.Semaphore(max(1, concurrency))
    tallies = {"active": 0, "dead": 0, "restored_failed": 0, "no_backup": 0}
    lock = asyncio.Lock()

    async def one(row: dict) -> None:
        phone = row["phone"]
        uniques: list[tuple[str, str, dict]] = row["uniques"]
        if not uniques:
            log.info(f"🧩 +{phone} · no valid Telethon string in backups")
            async with lock:
                tallies["no_backup"] += 1
            return

        log.info(
            f"🔑 +{phone} · {len(uniques)} unique valid key(s) from "
            f"{uniques[0][0]} · fp={_fp(uniques[0][1])} · "
            f"backups={row['backup_docs']} (duplicates skipped)"
        )

        chosen_src, chosen_str, _ = uniques[0]
        verdict = "restored_failed"
        if live:
            async with sem:
                last = "skip"
                for source, session_str, _meta in uniques:
                    last = await _live_verdict(
                        session_str,
                        int(row["api_id"] or 0),
                        str(row["api_hash"] or ""),
                        row["device"],
                        sticky=phone,
                    )
                    if last == "active":
                        chosen_src, chosen_str = source, session_str
                        verdict = "active"
                        log.info(f"✅ +{phone} · profile fetched → ACTIVE ({source})")
                        break
                    if last == "dead":
                        log.info(
                            f"🪦 +{phone} · {source} unauthorized/two-IP/no profile"
                        )
                        continue
                    log.info(f"⏭️ +{phone} · {source} network skip")
                else:
                    if last == "dead":
                        verdict = "dead"
                        log.info(f"🪦 +{phone} · all unique keys dead → DEAD")
                    else:
                        verdict = "restored_failed"
                        log.info(
                            f"⏭️ +{phone} · live check skipped (network) · "
                            f"string restored only ({chosen_src})"
                        )
        else:
            verdict = "restored_failed"

        if apply:
            _write_account(src, row, chosen_str, verdict)

        async with lock:
            tallies[verdict] += 1

    await asyncio.gather(*(one(r) for r in rows))
    return tallies


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore wrongly flagged bad-session accounts from session_backups."
    )
    parser.add_argument("--apply", action="store_true", help="Write chosen session back to source_accounts.")
    parser.add_argument("--live", action="store_true", help="Telegram get_me: profile=active, unauthorized/two-IP=dead.")
    parser.add_argument("--concurrency", type=int, default=8)
    args = parser.parse_args()

    client, src, backups = _mongo()
    try:
        rows = restore_docs(src, backups)
        with_valid = sum(1 for r in rows if r["uniques"])
        no_valid = len(rows) - with_valid
        log.info(
            f"🏥 flagged={len(rows)} · unique_valid_found={with_valid} · "
            f"no_usable_backup={no_valid} · apply={args.apply} · live={args.live}"
        )
        if not rows:
            log.info("Nothing flagged as invalid_session_string / Not a valid string.")
            return 0
        tallies = asyncio.run(maybe_live(rows, args.apply, args.live, src, args.concurrency))
        log.info(
            f"📊 result · active={tallies['active']} · dead={tallies['dead']} · "
            f"string_restored_no_live={tallies['restored_failed']} · "
            f"no_backup={tallies['no_backup']}"
        )
        if not args.apply:
            log.info("Dry-run only. Re-run with --apply --live to write DB after get_me.")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
