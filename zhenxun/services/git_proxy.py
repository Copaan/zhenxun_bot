"""Process-local policy for the plugin store's managed HTTP Git fetches."""

import os
from urllib.parse import urlsplit

from zhenxun.services.network_proxy import ProxyPolicy, ProxyPolicyError


def git_download_environment(
    policy: ProxyPolicy, url: str, authentication: dict[str, str] | None = None
) -> tuple[dict[str, str], str]:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ProxyPolicyError("proxy_git_protocol_unmanaged")
    bypass = policy.bypasses(parsed.hostname)
    forced = policy.selected(None) and not bypass
    proxy = policy.proxy if (forced or policy.mode == "legacy") and not bypass else None
    if forced and not proxy:
        raise ProxyPolicyError("proxy_configuration_unavailable")
    if proxy and proxy.startswith("socks5://"):
        proxy = "socks5h://" + proxy[len("socks5://") :]
    # Neither user-wide Git settings nor inherited NO_PROXY may defeat a forced route.
    environment = {
        key: value
        for key, value in os.environ.items()
        if not (
            key.upper().endswith("_PROXY")
            or key.upper().startswith("GIT_CONFIG")
            or key.upper()
            in {"GIT_SSL_NO_VERIFY", "GIT_PROXY_COMMAND", "GIT_ASKPASS", "SSH_ASKPASS"}
        )
    }
    environment.update(
        {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
        }
    )
    config = []
    if authentication:
        for i in range(int(authentication.get("GIT_CONFIG_COUNT", "0"))):
            config.append(
                (
                    authentication[f"GIT_CONFIG_KEY_{i}"],
                    authentication[f"GIT_CONFIG_VALUE_{i}"],
                )
            )
    config.extend(
        [
            ("http.proxy", proxy or ""),
            ("remote.origin.proxy", proxy or ""),
            ("http.sslVerify", "true"),
            ("http.followRedirects", "false"),
            ("credential.helper", ""),
            ("protocol.allow", "never"),
            ("protocol.http.allow", "always"),
            ("protocol.https.allow", "always"),
        ]
    )
    environment["GIT_CONFIG_COUNT"] = str(len(config))
    for i, (key, value) in enumerate(config):
        environment[f"GIT_CONFIG_KEY_{i}"] = key
        environment[f"GIT_CONFIG_VALUE_{i}"] = value
    route = (
        "local_direct"
        if bypass
        else "forced_proxy"
        if forced
        else "legacy_proxy"
        if proxy
        else "direct"
    )
    return environment, route
