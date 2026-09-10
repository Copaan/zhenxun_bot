"""Reversible proxy routing at supported third-party client request boundaries."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from functools import wraps
import importlib
import inspect
import ssl
from threading import RLock
from urllib.parse import urlsplit

from .network_proxy import ProxyPolicyError, proxy_runtime, request_owner

_scope = ContextVar("third_party_proxy_scope", default=None)
_patches = []
_lock = RLock()
coverage = {}
_ytdlp_adapter = None


class _PrivateProxy(str):
    def __repr__(self):
        return "<proxy>"


@contextmanager
def request_scope():
    token = None
    if _scope.get() is None:
        token = _scope.set((proxy_runtime.policy_snapshot(), request_owner()))
    try:
        yield
    finally:
        if token is not None:
            _scope.reset(token)


def route(url):
    policy, owner = _scope.get() or (proxy_runtime.policy_snapshot(), request_owner())
    if not policy.selected(owner):
        return False, None
    if proxy_runtime.stopping:
        raise ProxyPolicyError("proxy_runtime_stopping")
    parsed = urlsplit(str(url))
    if parsed.scheme not in {"http", "https", "ws", "wss"}:
        raise ProxyPolicyError("proxy_protocol_unmanaged")
    if policy.bypasses(parsed.hostname or ""):
        return True, None
    if not policy.proxy:
        raise ProxyPolicyError("proxy_configuration_unavailable")
    return True, _PrivateProxy(policy.proxy)


def _patch(target, name, wrapped):
    original = getattr(target, name)
    _patches.append((target, name, original, wrapped))
    setattr(target, name, wrapped)


def _require_verified_context(value):
    if isinstance(value, ssl.SSLContext) and (
        value.verify_mode != ssl.CERT_REQUIRED or not value.check_hostname
    ):
        raise ProxyPolicyError("proxy_tls_verification_required")


def _requests():
    import requests

    original = requests.Session.send

    @wraps(original)
    def send(client, request, **kwargs):
        with request_scope():
            managed, proxy = route(request.url)
            if managed:
                # requests.rebuild_proxies may leave proxy credentials on a
                # redirected request even when the next hop bypasses the proxy.
                request.headers.pop("Proxy-Authorization", None)
                adapter = client.get_adapter(request.url)
                if type(adapter) not in {requests.adapters.HTTPAdapter, _ytdlp_adapter}:
                    raise ProxyPolicyError("proxy_custom_transport_unmanaged")
                if type(adapter) is _ytdlp_adapter:
                    contexts = [
                        adapter._pm_args.get("ssl_context"),
                        adapter._proxy_ssl_context,
                    ]
                    if any(
                        context is not None
                        and (
                            context.verify_mode != ssl.CERT_REQUIRED
                            or not context.check_hostname
                        )
                        for context in contexts
                    ):
                        raise ProxyPolicyError("proxy_tls_verification_required")
                kwargs["proxies"] = {"http": proxy, "https": proxy}
                kwargs["verify"] = kwargs.get("verify") or True
            try:
                response = original(client, request, **kwargs)
                if managed and proxy and response.status_code == 407:
                    response.close()
                    raise ProxyPolicyError("proxy_auth_failed")
                return response
            except requests.RequestException as error:
                if managed and proxy:
                    if isinstance(error, requests.exceptions.InvalidSchema):
                        raise ProxyPolicyError(
                            "proxy_requests_transport_unavailable"
                        ) from None
                    code = (
                        "proxy_timeout"
                        if isinstance(error, requests.Timeout)
                        else "proxy_request_failed"
                    )
                    raise ProxyPolicyError(code) from None
                raise

    _patch(requests.Session, "send", send)


def _httpx(client_type, transport_type, asynchronous):
    import httpx

    init = client_type.__init__
    select = client_type._transport_for_url
    close = client_type.aclose if asynchronous else client_type.close
    send = client_type.send
    signature = inspect.signature(init)

    @wraps(init)
    def initialize(client, *args, **kwargs):
        bound = signature.bind(client, *args, **kwargs)
        options = bound.arguments
        client._zx_proxy_custom = bool(
            options.get("transport") or options.get("mounts")
        )
        client._zx_proxy_options = {
            key: options[key]
            for key in ("verify", "cert", "http1", "http2", "limits")
            if key in options
        }
        if client._zx_proxy_options.get("verify") is False:
            client._zx_proxy_options["verify"] = True
        client._zx_proxy_transports = {}
        init(client, *args, **kwargs)

    @wraps(select)
    def transport(client, url):
        from .network_proxy import ManagedAsyncClient

        if isinstance(client, ManagedAsyncClient):
            return select(client, url)
        managed, proxy = route(url)
        if not managed:
            return select(client, url)
        if getattr(client, "_zx_proxy_custom", True):
            raise ProxyPolicyError("proxy_custom_transport_unmanaged")
        _require_verified_context(client._zx_proxy_options.get("verify"))
        with _lock:
            pools = client._zx_proxy_transports
            if proxy not in pools:
                try:
                    pools[proxy] = transport_type(
                        proxy=proxy, trust_env=False, **client._zx_proxy_options
                    )
                except (ImportError, ValueError):
                    raise ProxyPolicyError("proxy_transport_unavailable") from None
            return pools[proxy]

    @wraps(send)
    def sync_send(client, request, **kwargs):
        with request_scope():
            try:
                return send(client, request, **kwargs)
            except httpx.RequestError:
                if route(request.url)[1]:
                    raise ProxyPolicyError("proxy_request_failed") from None
                raise

    @wraps(send)
    async def async_send(client, request, **kwargs):
        with request_scope():
            try:
                return await send(client, request, **kwargs)
            except httpx.RequestError:
                if route(request.url)[1]:
                    raise ProxyPolicyError("proxy_request_failed") from None
                raise

    @wraps(close)
    def sync_close(client):
        try:
            for pool in getattr(client, "_zx_proxy_transports", {}).values():
                pool.close()
        finally:
            close(client)

    @wraps(close)
    async def async_close(client):
        try:
            for pool in getattr(client, "_zx_proxy_transports", {}).values():
                await pool.aclose()
        finally:
            await close(client)

    _patch(client_type, "__init__", initialize)
    _patch(client_type, "_transport_for_url", transport)
    _patch(client_type, "send", async_send if asynchronous else sync_send)
    _patch(
        client_type,
        "aclose" if asynchronous else "close",
        async_close if asynchronous else sync_close,
    )


def _aiohttp():
    import aiohttp
    from yarl import URL

    request_init = aiohttp.ClientRequest.__init__
    request = aiohttp.ClientSession._request

    @wraps(request_init)
    def initialize(client, method, url, **kwargs):
        managed, proxy = route(url)
        if managed:
            _require_verified_context(kwargs.get("ssl"))
            if proxy and urlsplit(proxy).scheme not in {"http", "https"}:
                raise ProxyPolicyError("proxy_aiohttp_protocol_unsupported")
            kwargs.update(
                proxy=URL(proxy) if proxy else None,
                proxy_auth=None,
                proxy_headers=None,
                trust_env=False,
            )
            if kwargs.get("ssl") is False:
                kwargs["ssl"] = True
        request_init(client, method, url, **kwargs)

    @wraps(request)
    async def send(client, method, url, **kwargs):
        with request_scope():
            managed, proxy = route(client._build_url(url))
            if managed and (
                type(client.connector) is not aiohttp.TCPConnector
                or client._request_class is not aiohttp.ClientRequest
            ):
                raise ProxyPolicyError("proxy_custom_transport_unmanaged")
            if managed:
                if client.connector._ssl is False:
                    raise ProxyPolicyError("proxy_tls_verification_required")
                _require_verified_context(client.connector._ssl)
            try:
                return await request(client, method, url, **kwargs)
            except aiohttp.ClientError:
                if managed and proxy:
                    raise ProxyPolicyError("proxy_request_failed") from None
                raise

    _patch(aiohttp.ClientRequest, "__init__", initialize)
    _patch(aiohttp.ClientSession, "_request", send)


def _ytdlp():
    global _ytdlp_adapter

    from yt_dlp import YoutubeDL
    from yt_dlp.networking.common import RequestDirector

    if importlib.util.find_spec("requests") is not None:
        from yt_dlp.networking._requests import RequestsHTTPAdapter

        _ytdlp_adapter = RequestsHTTPAdapter

    direct = RequestDirector.send

    @wraps(direct)
    def dispatch(director, request):
        if route(request.url)[0]:
            handler = director.handlers.get("Requests")
            if handler is None:
                raise ProxyPolicyError("proxy_ytdlp_requests_unavailable")
            # curl/urllib fallback would escape request-boundary redirect policy.
            handler.validate(request)
            return handler.send(request)
        return direct(director, request)

    _patch(RequestDirector, "send", dispatch)

    original = YoutubeDL.urlopen

    @wraps(original)
    def urlopen(client, request):
        with request_scope():
            from yt_dlp.networking import Request

            if isinstance(request, str):
                request = Request(request)
            url = getattr(request, "url", None) or request.full_url
            managed, proxy = route(url)
            if managed:
                if not isinstance(request, Request):
                    raise ProxyPolicyError("proxy_ytdlp_request_unsupported")
                request = request.copy()
                request.proxies = {"all": proxy or ""}
            return original(client, request)

    _patch(YoutubeDL, "urlopen", urlopen)


def install():
    if _patches:
        return
    from concurrent.futures import ThreadPoolExecutor

    import httpx

    try:
        _httpx(httpx.Client, httpx.HTTPTransport, False)
        _httpx(httpx.AsyncClient, httpx.AsyncHTTPTransport, True)
        coverage["httpx"] = "enabled"
        coverage["httpx_socks"] = (
            "enabled" if importlib.util.find_spec("socksio") else "not_installed"
        )
        coverage["requests_socks"] = (
            "enabled" if importlib.util.find_spec("socks") else "not_installed"
        )
        for name, setup in (
            ("requests", _requests),
            ("aiohttp", _aiohttp),
            ("yt_dlp", _ytdlp),
        ):
            if importlib.util.find_spec(name) is None:
                coverage[name] = "not_installed"
            else:
                setup()
                coverage[name] = "enabled"
        submit = ThreadPoolExecutor.submit

        @wraps(submit)
        def contextual_submit(pool, function, /, *args, **kwargs):
            return submit(pool, copy_context().run, function, *args, **kwargs)

        _patch(ThreadPoolExecutor, "submit", contextual_submit)
    except BaseException:
        restore()
        raise


def restore():
    global _ytdlp_adapter

    for target, name, original, wrapped in reversed(_patches):
        if getattr(target, name) is wrapped:
            setattr(target, name, original)
    _patches.clear()
    coverage.clear()
    _ytdlp_adapter = None
