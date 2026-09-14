import pytest

from exception_classifier import ErrorCategory, classify_exception


telethon_errors = pytest.importorskip("telethon.errors")


@pytest.mark.parametrize(
    ("error_name", "category"),
    [
        ("UserBannedInChannelError", ErrorCategory.ACCOUNT_RESTRICTED),
        ("ChannelPrivateError", ErrorCategory.TARGET_UNAVAILABLE),
    ],
)
def test_chat_restrictions_are_not_account_quarantines(error_name, category):
    error_type = getattr(telethon_errors, error_name, None)
    if error_type is None:
        pytest.skip(f"{error_name} is unavailable in this Telethon version")

    result = classify_exception(error_type(request=None))

    assert result.category is category
    assert result.is_quarantinable is False
    assert result.terminal is False


@pytest.mark.parametrize("message", ["banned", "blocked", "revoked"])
def test_string_fallback_does_not_promote_broad_keywords(message):
    result = classify_exception(RuntimeError(message))

    assert result.category is not ErrorCategory.ACCOUNT_BANNED