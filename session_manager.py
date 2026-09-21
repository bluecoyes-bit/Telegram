"""Backward-compatible session lifecycle imports."""

from exception_classifier import ErrorCategory
from resource_manager import (
    SessionAlreadyOwnedError,
    SessionLifecycleState,
    SessionManager,
)

__all__ = [
    "ErrorCategory",
    "SessionAlreadyOwnedError",
    "SessionLifecycleState",
    "SessionManager",
]
