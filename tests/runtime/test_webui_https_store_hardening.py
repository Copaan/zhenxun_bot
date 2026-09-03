from __future__ import annotations

import pytest


def test_webui_tls_redirect_requires_https_and_launcher() -> None:
    from zhenxun.configs.webui_tls import (
        WebUITLSConfigError,
        WebUITLSSettings,
        validate_webui_tls_settings,
    )

    with pytest.raises(WebUITLSConfigError, match="必须先启用"):
        validate_webui_tls_settings(
            WebUITLSSettings(redirect_enabled=True), launcher_managed=True
        )
    with pytest.raises(WebUITLSConfigError, match="launcher"):
        validate_webui_tls_settings(
            WebUITLSSettings(
                enabled=True,
                certfile="missing.pem",
                keyfile="missing.key",
                redirect_enabled=True,
            ),
            launcher_managed=False,
        )


def test_store_version_state_is_not_raw_string_inequality() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _install_state,
    )

    state, update, _ = _install_state(
        installed=True,
        installed_version="v1.0.0",
        catalog_version="1.0.0",
        digest="same",
        receipt=None,
    )
    assert (state, update) == ("installed", False)

    state, update, reason = _install_state(
        installed=True,
        installed_version="1.0.0",
        catalog_version="1.1.0",
        digest="changed",
        receipt={"catalog_version": "1.0.0", "source_digest": "original"},
    )
    assert (state, update, reason) == (
        "locally_modified",
        True,
        "local_source_changed",
    )


def test_placeholder_provider_keys_are_not_configured() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration import (
        _is_placeholder_secret,
        _provider_view,
    )
    from zhenxun.services.ai.config.models import ProviderConfig

    assert _is_placeholder_secret("YOUR_ARK_API_KEY")
    assert _is_placeholder_secret("${ARK_API_KEY}")
    assert not _is_placeholder_secret("sk-live-value")
    view = _provider_view(
        ProviderConfig(
            name="Ark",
            api_key=["YOUR_ARK_API_KEY", "sk-live-value"],
            api_base="https://example.invalid/v1",
            models=[{"model_name": "model"}],
        )
    )
    assert view["api_key_slots"] == [{"existing_index": 1, "configured": True}]


def test_redirect_hostname_accepts_normal_hosts_and_rejects_unsafe_values() -> None:
    from zhenxun.cli import _safe_redirect_hostname

    assert _safe_redirect_hostname("192.168.1.8:80") == "192.168.1.8"
    assert _safe_redirect_hostname("[fd00::8]:80") == "[fd00::8]"
    assert _safe_redirect_hostname("example.com:8080") == "example.com"
    assert _safe_redirect_hostname("bad_host.example") == "localhost"
    assert _safe_redirect_hostname("\u4f8b\u5b50.com") == "localhost"


def test_store_errors_redact_credentials() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _safe_store_error,
    )

    message = _safe_store_error(
        RuntimeError("https://oauth2:secret@example.com/repo?token=private")
    )
    assert "secret" not in message
    assert "private" not in message


def test_git_output_redacts_credentials_and_query_tokens() -> None:
    from zhenxun.utils.repo_utils.utils import redact_git_output

    value = redact_git_output(
        "From https://oauth2:secret@example.com/repo?access_token=token "
        "Authorization: Bearer another-secret"
    )
    assert "secret" not in value
    assert "token" not in value.replace("access_token", "")
    assert "[credentials]@example.com" in value
    assert "Authorization: [redacted]" in value
