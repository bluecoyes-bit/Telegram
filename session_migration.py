"""OFFLINE SESSION MIGRATION UTILITY (P0-CLOSEOUT).

This module is ISOLATED from normal runtime. It performs one-time migration of
local Telethon session files (the `.session` files on disk + `vars.txt` API
credentials) into the DB's `source_accounts` collection.

It is the ONLY place (outside SessionManager) that constructs a TelegramClient
for a user session, and it is deliberately:
  * NOT imported by startup, recovery, commands, or web APIs
  * NOT a member of SessionManager's runtime client pool
  * NOT run during normal operation

It exists solely because migration needs a raw `StringSession` from existing
`.session` files on disk — an operation that has no authorized session_string in
the DB yet, so SessionManager.acquire() cannot serve it.

If this module is ever referenced from normal runtime code, that reference is a
bug.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import random
from typing import Any, Dict, List, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession

from config import DEVICE_PROFILES


async def migrate_local_sessions(
    db: Any,
    event=None,
    sessions_dir: str = "sessions",
    vars_path: str = "vars.txt",
    json_2fa_path: str = "twofa_passwords.json",
) -> Dict[str, Any]:
    """
    Process local session files -> DB source_accounts.

    db must provide:
      - parse_vars_txt(vars_path) -> {phone: {"api_id": int, "api_hash": str}}
      - resolve_session_path(phone, sessions_path) -> path or None
      - save_authorized_session(phone=..., session_str=..., status=..., device=..., two_fa_password=...)
      - _normalize(k) -> clean phone key
    """
    vars_data = db.parse_vars_txt(vars_path)
    sessions_path = pathlib.Path(sessions_dir)
    sessions_path.mkdir(parents=True, exist_ok=True)

    staged = migrated = failed = skipped = 0
    errors: List[Dict[str, str]] = []

    twofa_map: Dict[str, str] = {}
    json_file = pathlib.Path(json_2fa_path)
    if json_file.exists() and json_file.stat().st_size > 0:
        try:
            with open(json_file, "r", encoding="utf-8") as jf:
                raw_json_data = json.load(jf)
                if isinstance(raw_json_data, dict):
                    for k, v in raw_json_data.items():
                        clean_k = db._normalize(k)
                        if clean_k:
                            twofa_map[clean_k] = str(v).strip()
        except Exception as json_err:
            errors.append(
                {"phone": "JSON_Config", "error": f"JSON parse error: {str(json_err)[:100]}"}
            )

    if not vars_data:
        return {
            "staged": 0,
            "migrated": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [{"phone": "All", "error": "vars.txt missing or empty."}],
        }

    total_accounts = len(vars_data)
    processed_count = 0

    for phone, creds in vars_data.items():
        processed_count += 1
        clean_phone_key = db._normalize(phone)
        session_path = db.resolve_session_path(phone, sessions_path)

        if event:
            try:
                await event.edit(
                    f"⏳ **Live Account Sync...**\n\n"
                    f"🔄 `[{processed_count}/{total_accounts}]`\n"
                    f"🟢 Migrated: `{migrated}`\n"
                    f"🔴 Failed: `{failed}`\n"
                    f"🟡 Skipped: `{skipped}`\n"
                    f"⚙️ `+{clean_phone_key}`"
                )
            except Exception:
                pass

        if not session_path:
            skipped += 1
            errors.append({"phone": phone, "error": "Session file missing."})
            continue

        device = random.choice(DEVICE_PROFILES) if DEVICE_PROFILES else {
            "device_model": "PC 64bit",
            "system_version": "Windows 11 Pro 23H2",
            "app_version": "5.1.0",
        }

        # Migration-only client — raw .session on disk, no DB session_string yet.
        client = TelegramClient(
            str(session_path),
            int(creds["api_id"]),
            str(creds["api_hash"]),
            device_model=device["device_model"],
            system_version=device["system_version"],
            app_version=device["app_version"],
        )

        try:
            await client.connect()
            if not await client.is_user_authorized():
                failed += 1
                errors.append({"phone": phone, "error": "Session unauthorized."})
                await client.disconnect()
                await asyncio.sleep(random.uniform(0.5, 1.5))
                continue

            session_str = StringSession.save(client.session)
            matched_2fa = twofa_map.get(clean_phone_key, None)

            db.save_authorized_session(
                phone=phone,
                session_str=session_str,
                status="active",
                device=device,
                two_fa_password=matched_2fa,
            )

            staged += 1
            migrated += 1

        except Exception as exc:
            failed += 1
            errors.append({"phone": phone, "error": str(exc)[:100]})
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(random.uniform(0.3, 1.0))

    return {
        "staged": staged,
        "migrated": migrated,
        "failed": failed,
        "skipped": skipped,
        "errors": errors,
    }
