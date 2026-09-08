#!/usr/bin/env python3
"""
Centralized exception classification for the Telegram multi-account system.

Provides structured classification of Telegram / Telethon / network errors
into a ConnectionResult that distinguishes retryable vs terminal states,
and specifically isolates AuthKeyDuplicatedError from ordinary failures.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from telethon.errors import (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    PhoneNumberBannedError,
    FloodWaitError,
    PeerFloodError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
    UserAlreadyParticipantError,
    ChatAdminRequiredError,
    AuthKeyDuplicatedError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    SessionPasswordNeededError,
)

logger = logging.getLogger("ExceptionClassifier")


class ErrorCategory(enum.Enum):
    AUTH_KEY_DUPLICATED = "auth_key_duplicated"
    AUTH_KEY_UNREGISTERED = "auth_key_unregistered"
    SESSION_REVOKED = "session_revoked"
    UNAUTHORIZED = "unauthorized"
    ACCOUNT_BANNED = "account_banned"
    ACCOUNT_FLOOD = "account_flood"
    PEER_FLOOD = "peer_flood"
    USER_PRIVACY_RESTRICTED = "user_privacy_restricted"
    USER_BLOCKED = "user_blocked"
    ALREADY_PARTICIPANT = "already_participant"
    CHAT_ADMIN_REQUIRED = "chat_admin_required"
    PHONE_CODE_INVALID = "phone_code_invalid"
    PHONE_CODE_EXPIRED = "phone_code_expired"
    PASSWORD_INVALID = "password_invalid"
    PASSWORD_NEEDED = "password_needed"
    NETWORK_TIMEOUT = "network_timeout"
    PROXY_ERROR = "proxy_error"
    CONNECTION_ERROR = "connection_error"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ConnectionResult:
    """Structured result of an exception classification."""

    success: bool
    category: ErrorCategory
    retryable: bool
    terminal: bool
    reason: str
    original_exception: Optional[BaseException] = None

    @property
    def is_quarantinable(self) -> bool:
        """True when the account/session should be quarantined."""
        return self.category in (
            ErrorCategory.AUTH_KEY_DUPLICATED,
            ErrorCategory.SESSION_REVOKED,
            ErrorCategory.AUTH_KEY_UNREGISTERED,
            ErrorCategory.ACCOUNT_BANNED,
            ErrorCategory.UNAUTHORIZED,
        )


def classify_exception(exc: BaseException) -> ConnectionResult:
    """
    Classify any exception into a structured ConnectionResult.

    Rules:
      - AuthKeyDuplicatedError -> terminal, quarantinable, NOT retryable
      - AuthKeyUnregisteredError / SessionRevokedError -> terminal, quarantinable
      - UserDeactivated* / PhoneNumberBannedError -> terminal, quarantinable
      - FloodWaitError / PeerFloodError -> retryable but requires cooldown
      - UserPrivacyRestrictedError / UserIsBlockedError -> not retryable, target issue
      - Network / proxy errors -> retryable (limited)
    """

    if isinstance(exc, AuthKeyDuplicatedError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.AUTH_KEY_DUPLICATED,
            retryable=False,
            terminal=True,
            reason="Authorization key used under two different IPs simultaneously",
            original_exception=exc,
        )

    if isinstance(exc, (AuthKeyUnregisteredError, SessionRevokedError)):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.AUTH_KEY_UNREGISTERED,
            retryable=False,
            terminal=True,
            reason="Session auth key unregistered or revoked",
            original_exception=exc,
        )

    if isinstance(exc, (UserDeactivatedError, UserDeactivatedBanError)):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.ACCOUNT_BANNED,
            retryable=False,
            terminal=True,
            reason="Account deactivated/banned by Telegram",
            original_exception=exc,
        )

    if isinstance(exc, PhoneNumberBannedError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.ACCOUNT_BANNED,
            retryable=False,
            terminal=True,
            reason="Phone number banned by Telegram",
            original_exception=exc,
        )

    if isinstance(exc, FloodWaitError):
        seconds = getattr(exc, "seconds", 0) or 0
        return ConnectionResult(
            success=False,
            category=ErrorCategory.ACCOUNT_FLOOD,
            retryable=True,
            terminal=False,
            reason=f"FloodWait for {seconds}s",
            original_exception=exc,
        )

    if isinstance(exc, PeerFloodError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.PEER_FLOOD,
            retryable=True,
            terminal=False,
            reason="Peer flood error",
            original_exception=exc,
        )

    if isinstance(exc, UserPrivacyRestrictedError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.USER_PRIVACY_RESTRICTED,
            retryable=False,
            terminal=False,
            reason="Target user privacy restricted",
            original_exception=exc,
        )

    if isinstance(exc, UserIsBlockedError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.USER_BLOCKED,
            retryable=False,
            terminal=False,
            reason="User is blocked",
            original_exception=exc,
        )

    if isinstance(exc, UserAlreadyParticipantError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.ALREADY_PARTICIPANT,
            retryable=False,
            terminal=False,
            reason="Already participant in target",
            original_exception=exc,
        )

    if isinstance(exc, ChatAdminRequiredError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.CHAT_ADMIN_REQUIRED,
            retryable=False,
            terminal=False,
            reason="Admin rights required for this operation",
            original_exception=exc,
        )

    if isinstance(exc, (PhoneCodeInvalidError,)):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.PHONE_CODE_INVALID,
            retryable=True,
            terminal=False,
            reason="OTP code invalid",
            original_exception=exc,
        )

    if isinstance(exc, (PhoneCodeExpiredError,)):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.PHONE_CODE_EXPIRED,
            retryable=True,
            terminal=False,
            reason="OTP code expired",
            original_exception=exc,
        )

    if isinstance(exc, (PasswordHashInvalidError,)):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.PASSWORD_INVALID,
            retryable=True,
            terminal=False,
            reason="2FA password invalid",
            original_exception=exc,
        )

    if isinstance(exc, SessionPasswordNeededError):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.PASSWORD_NEEDED,
            retryable=False,
            terminal=False,
            reason="2FA password required",
            original_exception=exc,
        )

    if isinstance(exc, asyncio.TimeoutError):  # type: ignore[name-defined]
        return ConnectionResult(
            success=False,
            category=ErrorCategory.NETWORK_TIMEOUT,
            retryable=True,
            terminal=False,
            reason="Operation timed out",
            original_exception=exc,
        )

    # String-based fallbacks for errors not covered by Telethon exception types
    err_str = str(exc).lower()

    if "auth_key_duplicated" in err_str or "duplicated" in err_str:
        return ConnectionResult(
            success=False,
            category=ErrorCategory.AUTH_KEY_DUPLICATED,
            retryable=False,
            terminal=True,
            reason="Authorization key used under two different IPs simultaneously",
            original_exception=exc,
        )

    if any(k in err_str for k in ("authkeyunregistered", "sessionrevoked", "expired", "unauthorized")):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.AUTH_KEY_UNREGISTERED,
            retryable=False,
            terminal=True,
            reason=f"Session revoked/unregistered: {err_str[:80]}",
            original_exception=exc,
        )

    if any(k in err_str for k in ("userdeactivated", "banned", "disabled", "blocked", "revoked")):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.ACCOUNT_BANNED,
            retryable=False,
            terminal=True,
            reason=f"Account terminated: {err_str[:80]}",
            original_exception=exc,
        )

    if any(k in err_str for k in ("flood", "rate limit", "too many", "spam")):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.ACCOUNT_FLOOD,
            retryable=True,
            terminal=False,
            reason=f"Rate-limited: {err_str[:80]}",
            original_exception=exc,
        )

    if any(k in err_str for k in ("proxy", "socks", "connection refused", "connection reset", "network", "dns")):
        return ConnectionResult(
            success=False,
            category=ErrorCategory.NETWORK_TIMEOUT,
            retryable=True,
            terminal=False,
            reason=f"Network/proxy error: {err_str[:80]}",
            original_exception=exc,
        )

    return ConnectionResult(
        success=False,
        category=ErrorCategory.UNKNOWN,
        retryable=False,
        terminal=False,
        reason=f"Unclassified: {err_str[:120]}",
        original_exception=exc,
    )

def is_retryable_error(exc: Exception) -> bool:
    """
    Return True when an exception is safe to retry.
    Return False for permanent/auth/permission failures.
    """
    name = type(exc).__name__.lower()
    message = str(exc).lower()

    permanent = (
        "authkeyunregistered",
        "sessionrevoked",
        "phonenumberbanned",
        "userdeactivated",
        "userdeactivatedban",
        "chatwriteforbidden",
        "channelprivate",
        "channelinvalid",
        "usernameinvalid",
        "usernameoccupied",
    )

    if any(x in name for x in permanent):
        return False

    temporary = (
        "timeout",
        "connection",
        "network",
        "server",
        "rpc",
        "floodwait",
        "limit",
    )

    if any(x in name for x in temporary):
        return True

    if any(x in message for x in temporary):
        return True

    return False

def classify_connection_error(exc: BaseException) -> Tuple[bool, str]:
    """
    Backward-compatible helper that returns (can_recover, status_string).

    Used by the auditor and recovery loops.
    """
    result = classify_exception(exc)
    if result.category == ErrorCategory.AUTH_KEY_DUPLICATED:
        return False, "auth_key_duplicated"
    if result.category == ErrorCategory.AUTH_KEY_UNREGISTERED:
        return False, "revoked"
    if result.category == ErrorCategory.ACCOUNT_BANNED:
        return False, "banned"
    if result.category == ErrorCategory.ACCOUNT_FLOOD:
        return True, "flood"
    if result.category == ErrorCategory.NETWORK_TIMEOUT:
        return True, "connection_error"
    if result.category == ErrorCategory.UNAUTHORIZED:
        return False, "unauthorized"
    if result.success:
        return True, "authorized"
    return False, "unknown"

