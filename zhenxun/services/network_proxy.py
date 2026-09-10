"""Shared routing policy for managed HTTP and supported client compatibility hooks."""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass, field
from functools import wraps
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import time
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from dotenv import dotenv_values
import httpx

PROXY_KEYS = frozenset(
    {
        "SYSTEM_PROXY",
        "NETWORK_PROXY_MODE",
        "NETWORK_PROXY_PLUGINS",
        "NETWORK_PROXY_CORE_ENABLED",
        "NETWORK_PROXY_BYPASS",
    }
)
DEFAULT_BYPASS = (
    "localhost",
    ".localhost",
    "127.0.0.0/8",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "::1/128",
    "fc00::/7",
    "fe80::/10",
)
PROBE_URL = "https://www.google.com/"
_AUTO_OWNER = object()


class ProxyPolicyError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class ProxyRequestError(httpx.RequestError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def normalize_proxy(value: str) -> str:
    if not value.strip():
        return ""
    try:
        parsed = urlsplit(value.strip())
        if parsed.scheme not in {"http", "https", "socks5", "socks5h"}:
            raise ValueError
        if not parsed.hostname or not parsed.port or parsed.query or parsed.fragment:
            raise ValueError
        if parsed.path not in {"", "/"} or any(c.isspace() for c in value):
            raise ValueError
        host = parsed.hostname.encode("idna").decode("ascii").lower()
        if any(c in host for c in "/\\@"):
            raise ValueError
        authority = f"[{host}]" if ":" in host else host
        auth = ""
        if parsed.username is not None:
            auth = quote(unquote(parsed.username), safe="")
            if parsed.password is not None:
                auth += ":" + quote(unquote(parsed.password), safe="")
            auth += "@"
        return urlunsplit(
            (parsed.scheme, f"{auth}{authority}:{parsed.port}", "", "", "")
        )
    except (ValueError, UnicodeError):
        raise ProxyPolicyError("proxy_address_invalid") from None


def public_proxy(value: str) -> dict:
    parsed = urlsplit(value)
    host = parsed.hostname or ""
    authority = f"[{host}]" if ":" in host else host
    return {
        "url": f"{parsed.scheme}://{authority}:{parsed.port}" if value else "",
        "has_auth": parsed.username is not None,
    }


def credential_proxy(
    url: str, saved: str, username: str | None, password: str | None, clear_auth: bool
) -> str:
    normalized = normalize_proxy(url)
    if not normalized:
        return ""
    if urlsplit(normalized).username is not None:
        raise ProxyPolicyError("proxy_use_separate_auth_fields")
    same = public_proxy(saved)["url"] == normalized
    previous = urlsplit(saved) if same else None
    if clear_auth:
        return normalized
    user = (
        username
        if username is not None
        else unquote(previous.username or "")
        if previous
        else ""
    )
    secret = (
        password
        if password is not None
        else unquote(previous.password or "")
        if previous
        else ""
    )
    if secret and not user:
        raise ProxyPolicyError("proxy_username_required")
    parsed = urlsplit(normalized)
    auth = f"{quote(user, safe='')}:{quote(secret, safe='')}@" if user else ""
    return urlunsplit((parsed.scheme, auth + parsed.netloc, "", "", ""))


def _strings(value, default=()) -> tuple[str, ...]:
    try:
        items = json.loads(value) if isinstance(value, str) else value
        items = list(default) if items is None else items
        if not isinstance(items, list) or len(items) > 2000:
            raise ValueError
        if any(not isinstance(item, str) or len(item) > 253 for item in items):
            raise ValueError
        return tuple(sorted({item.strip() for item in items if item.strip()}))
    except (ValueError, TypeError):
        raise ProxyPolicyError("proxy_list_invalid") from None


@dataclass(frozen=True)
class ProxyPolicy:
    mode: str = "disabled"
    proxy: str = field(default="", repr=False)
    plugins: tuple[str, ...] = ()
    core_enabled: bool = False
    bypass: tuple[str, ...] = DEFAULT_BYPASS

    @classmethod
    def from_values(cls, values: dict) -> ProxyPolicy:
        proxy = normalize_proxy(str(values.get("SYSTEM_PROXY") or ""))
        mode = str(
            values.get("NETWORK_PROXY_MODE") or ("legacy" if proxy else "disabled")
        )
        if mode not in {"legacy", "disabled", "global", "selected"}:
            raise ProxyPolicyError("proxy_mode_invalid")
        if mode in {"global", "selected"} and not proxy:
            raise ProxyPolicyError("proxy_address_required")
        plugins = _strings(values.get("NETWORK_PROXY_PLUGINS"))
        if any(not all(c.isalnum() or c in "_.:-" for c in owner) for owner in plugins):
            raise ProxyPolicyError("proxy_plugin_invalid")
        bypass = _strings(values.get("NETWORK_PROXY_BYPASS"), DEFAULT_BYPASS)
        for rule in bypass:
            try:
                if "/" in rule:
                    ipaddress.ip_network(rule, strict=False)
                else:
                    host = rule.lstrip(".")
                    if not host or any(c in host for c in "* /\\@?#"):
                        raise ValueError
                    if ":" in host:
                        ipaddress.ip_address(host)
                    host.encode("idna")
            except (ValueError, UnicodeError):
                raise ProxyPolicyError("proxy_bypass_invalid") from None
        enabled = str(values.get("NETWORK_PROXY_CORE_ENABLED") or "false").lower()
        if enabled not in {"true", "false", "1", "0"}:
            raise ProxyPolicyError("proxy_core_flag_invalid")
        return cls(mode, proxy, plugins, enabled in {"true", "1"}, bypass)

    @property
    def revision(self) -> str:
        values = [self.mode, self.proxy, self.plugins, self.core_enabled, self.bypass]
        return hashlib.sha256(json.dumps(values).encode()).hexdigest()

    def public(self) -> dict:
        return {
            "mode": self.mode,
            **public_proxy(self.proxy),
            "plugins": list(self.plugins),
            "core_enabled": self.core_enabled,
            "bypass": list(self.bypass),
            "revision": self.revision,
        }

    def selected(self, owner: str | None) -> bool:
        return self.mode == "global" or (
            self.mode == "selected"
            and (
                self.core_enabled
                if owner is None
                else any(
                    owner == plugin or owner.startswith((plugin + ".", plugin + ":"))
                    for plugin in self.plugins
                )
            )
        )

    def bypasses(self, host: str) -> bool:
        host = host.encode("idna").decode("ascii").lower().rstrip(".")
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        # Loopback management traffic cannot be forced through an external proxy.
        if (
            host == "localhost"
            or host.endswith(".localhost")
            or (address and address.is_loopback)
        ):
            return True
        for rule in self.bypass:
            if "/" in rule:
                if address and address in ipaddress.ip_network(rule, strict=False):
                    return True
            else:
                suffix = rule.startswith(".")
                rule = (
                    rule.lstrip(".").encode("idna").decode("ascii").lower().rstrip(".")
                )
                if suffix:
                    rule = "." + rule
                if host == rule or (
                    rule.startswith(".") and (host == rule[1:] or host.endswith(rule))
                ):
                    return True
        return False


def request_owner() -> str | None:
    from zhenxun.services.runtime_reload.ownership import current_owner

    return current_owner()


@dataclass(eq=False)
class PoolGeneration:
    policy: ProxyPolicy
    users: int = 0
    pools: dict = field(default_factory=dict, repr=False)


@dataclass
class RequestScope:
    runtime: Any
    generation: PoolGeneration
    owner: str | None
    task: Any
    live: bool = True


_request_scope: ContextVar[RequestScope | None] = ContextVar(
    "network_proxy_request", default=None
)


class ProxyRuntime:
    def __init__(self):
        self.current: PoolGeneration | None = None
        self.retired: list[PoolGeneration] = []
        self.stopping = False
        self.error_code: str | None = None
        self.counts: Counter = Counter()
        self.observed_owners: set[str] = set()
        self.probes: set[ProxyRuntime] = set()
        self._close_lock = asyncio.Lock()
        self._closing: dict[Any, asyncio.Task] = {}
        self._kernel = None

    def initialize(self, values=None):
        if self.current is None:
            if values is None:
                path = Path(".env.dev") if Path(".env.dev").exists() else Path(".env")
                values = dict(dotenv_values(path))
                values.update(
                    {key: os.environ[key] for key in PROXY_KEYS if key in os.environ}
                )
            try:
                policy = ProxyPolicy.from_values(values)
            except ProxyPolicyError as error:
                self.error_code = error.code
                policy = ProxyPolicy(mode="global")
            self.current = PoolGeneration(policy)
        return self.current

    def attach(self, context):
        self.initialize()
        self._kernel = context.kernel
        context.own_resource(
            receipt_id="network_proxy:pools",
            provider="httpx",
            resource_type="proxy_pools",
            release_check=lambda: self.stopping
            and not self.probes
            and all(not gen.pools and not gen.users for gen in self.generations()),
        )
        context.add_finalizer(self.shutdown)

    def generations(self):
        return [*self.retired, *([self.current] if self.current else [])]

    async def prepare(self, policy: ProxyPolicy):
        if self.stopping:
            raise ProxyPolicyError("proxy_runtime_stopping")
        self.initialize()
        await self.reap()
        if policy == self.current.policy:
            return self.current
        if len(self.retired) >= 2:
            raise ProxyPolicyError("proxy_pools_draining")
        if policy.proxy:
            try:
                transport = httpx.AsyncHTTPTransport(
                    proxy=policy.proxy, trust_env=False
                )
                await transport.aclose()
            except (ImportError, ValueError):
                raise ProxyPolicyError("proxy_transport_unavailable") from None
        return PoolGeneration(policy)

    def publish(self, prepared):
        if self.stopping:
            raise ProxyPolicyError("proxy_runtime_stopping")
        if prepared is not self.current:
            if self.current:
                self.retired.append(self.current)
            self.current = prepared
        self.error_code = None

    async def apply(self, values):
        self.publish(await self.prepare(ProxyPolicy.from_values(values)))
        # Publication is the commit point. Cleanup errors are diagnostics, not rollback.
        await self.reap(suppress_errors=True)

    async def close_transport(self, transport, timeout=2.0):
        from zhenxun.services.lifecycle.deadline import remaining_timeout

        timeout = remaining_timeout(timeout)
        if timeout <= 0:
            raise ProxyPolicyError("proxy_cleanup_budget_exhausted")
        task = self._closing.get(transport)
        if task is None:
            task = Context().run(
                asyncio.create_task, transport.aclose(), name="network-proxy:close"
            )
            self._closing[transport] = task
            task.add_done_callback(
                lambda completed: None
                if completed.cancelled()
                else completed.exception()
            )
        try:
            if self._kernel:
                await self._kernel._run_cleanup(
                    task,
                    owner="management:http_client",
                    stage="proxy_pool_close",
                    timeout=timeout,
                    grace=0,
                )
            else:
                done, _ = await asyncio.wait({task}, timeout=timeout)
                if not done:
                    task.cancel()
                    raise ProxyPolicyError("proxy_cleanup_budget_exhausted")
                task.result()
        except BaseException:
            self.error_code = "proxy_resources_unreleased"
            raise
        else:
            self._closing.pop(transport, None)

    async def reap(self, *, suppress_errors=False):
        async with self._close_lock:
            deadline = time.monotonic() + 2.0
            for generation in tuple(self.retired):
                if generation.users:
                    continue
                for key, transport in tuple(generation.pools.items()):
                    try:
                        await self.close_transport(
                            transport, deadline - time.monotonic()
                        )
                    except Exception:
                        if suppress_errors:
                            return
                        raise ProxyPolicyError("proxy_pools_draining") from None
                    generation.pools.pop(key, None)
                self.retired.remove(generation)

    @asynccontextmanager
    async def request_scope(self, *, owner=_AUTO_OWNER):
        if self.stopping:
            raise ProxyPolicyError("proxy_runtime_stopping")
        inherited = _request_scope.get()
        same_runtime = inherited and inherited.live and inherited.runtime is self
        if (
            same_runtime
            and inherited.task is asyncio.current_task()
            and owner is _AUTO_OWNER
        ):
            yield inherited
            return
        scope = RequestScope(
            self,
            inherited.generation if same_runtime else self.initialize(),
            request_owner() if owner is _AUTO_OWNER else owner,
            asyncio.current_task(),
        )
        scope.generation.users += 1
        token = _request_scope.set(scope)
        try:
            yield scope
        finally:
            scope.live = False
            _request_scope.reset(token)
            scope.generation.users -= 1
            await self.reap(suppress_errors=True)

    def cache_partition(self) -> str:
        scope = _request_scope.get()
        live = scope and scope.live and scope.runtime is self
        generation = scope.generation if live else self.initialize()
        owner = scope.owner if live else request_owner()
        return generation.policy.revision + ":" + (owner or "core")

    def policy_snapshot(self) -> ProxyPolicy:
        scope = _request_scope.get()
        if scope and scope.live and scope.runtime is self:
            return scope.generation.policy
        return self.initialize().policy

    def request_is_forced(self) -> bool:
        scope = _request_scope.get()
        if scope and scope.live and scope.runtime is self:
            return scope.generation.policy.selected(scope.owner)
        return self.initialize().policy.selected(request_owner())

    def status(self):
        from .proxy_clients import coverage

        current = self.initialize()
        return {
            "current": current.policy.public(),
            "error_code": self.error_code,
            "stopping": self.stopping,
            "retired_pools": len(self.retired),
            "active_requests": sum(gen.users for gen in self.generations()),
            "route_counts": dict(self.counts),
            "cleanup_tasks": len(self._closing),
            "active_probes": len(self.probes),
            "ownership_scope": "observed_call_context_only",
            "coverage": "managed_http_supported_clients_and_plugin_store_git",
            "clients": dict(coverage),
            "unmanaged": [
                "custom_transports_and_connectors",
                "external_downloaders",
                "browser",
                "unmanaged_git_pip_uv",
                "subprocesses",
                "raw_sockets",
            ],
        }

    async def shutdown(self, maximum=5.0):
        from zhenxun.services.lifecycle.deadline import remaining_timeout

        self.stopping = True
        deadline = time.monotonic() + remaining_timeout(maximum)
        probe_error = False
        for probe in tuple(self.probes):
            try:
                await probe.shutdown(max(0, deadline - time.monotonic()))
            except Exception:
                probe_error = True
            else:
                self.probes.discard(probe)
        while any(gen.users for gen in self.generations()):
            if time.monotonic() >= deadline:
                self.error_code = "proxy_resources_unreleased"
                raise ProxyPolicyError(self.error_code)
            await asyncio.sleep(min(0.05, max(0, deadline - time.monotonic())))
        for generation in self.generations():
            for key, transport in tuple(generation.pools.items()):
                if time.monotonic() >= deadline:
                    raise ProxyPolicyError("proxy_cleanup_budget_exhausted")
                await self.close_transport(transport, deadline - time.monotonic())
                generation.pools.pop(key, None)
        if probe_error:
            self.error_code = "proxy_probe_cleanup_unresolved"
            raise ProxyPolicyError(self.error_code)


proxy_runtime = ProxyRuntime()


def core_network_operation(function):
    @wraps(function)
    async def wrapped(*args, **kwargs):
        async with proxy_runtime.request_scope(owner=None):
            return await function(*args, **kwargs)

    return wrapped


class LeasedStream(httpx.AsyncByteStream):
    def __init__(self, stream, runtime, generation):
        self.stream, self.runtime, self.generation = stream, runtime, generation
        self.closed = False
        generation.users += 1

    async def __aiter__(self):
        async for chunk in self.stream:
            yield chunk

    async def aclose(self):
        if not self.closed:
            await self.stream.aclose()
            self.closed = True
            self.generation.users -= 1
            await self.runtime.reap(suppress_errors=True)


class RoutingTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, runtime, explicit, use_proxy, options, custom=None):
        self.runtime, self.explicit, self.use_proxy = runtime, explicit, use_proxy
        self.options, self.custom = options, custom
        self.closed = False

    async def handle_async_request(self, request):
        if self.closed:
            raise ProxyPolicyError("proxy_client_closed")
        scope = _request_scope.get()
        if scope is None or not scope.live:
            raise ProxyPolicyError("proxy_request_scope_missing")
        generation, owner = scope.generation, scope.owner
        policy = generation.policy
        forced = policy.selected(owner)
        bypass = policy.mode != "legacy" and policy.bypasses(request.url.host)
        if self.custom and forced:
            raise ProxyPolicyError("proxy_custom_transport_unmanaged")
        if forced and not bypass:
            if not policy.proxy:
                raise ProxyPolicyError("proxy_configuration_unavailable")
            proxy, route = policy.proxy, "forced_proxy"
        elif bypass:
            proxy, route = None, "local_direct"
        else:
            explicit = self.explicit
            if isinstance(explicit, dict):
                explicit = explicit.get(
                    request.url.scheme + "://", explicit.get("all://")
                )
            proxy = explicit or (
                policy.proxy if policy.mode == "legacy" and self.use_proxy else None
            )
            route = (
                "explicit_proxy" if explicit else "legacy_proxy" if proxy else "direct"
            )
        key = (self, proxy)
        transport = generation.pools.get(key)
        if transport is None:
            transport = self.custom or httpx.AsyncHTTPTransport(
                proxy=proxy, trust_env=False, **self.options
            )
            if not self.custom:
                generation.pools[key] = transport
        self.runtime.counts[route] += 1
        if owner and len(self.runtime.observed_owners) < 2000:
            self.runtime.observed_owners.add(owner)
        try:
            response = await transport.handle_async_request(request)
        except httpx.HTTPError as error:
            if not proxy:
                raise
            self.runtime.counts["proxy_error"] += 1
            code = (
                "proxy_auth_failed"
                if isinstance(error, httpx.ProxyError) and str(error).startswith("407")
                else "proxy_timeout"
                if isinstance(error, httpx.TimeoutException)
                else "proxy_connection_failed"
            )
            raise ProxyRequestError(code) from None
        if proxy and response.status_code == 407:
            await response.aclose()
            self.runtime.counts["proxy_error"] += 1
            raise ProxyRequestError("proxy_auth_failed")
        response.stream = LeasedStream(response.stream, self.runtime, generation)
        return response

    async def aclose(self):
        if self.closed:
            return
        if self.custom:
            await self.custom.aclose()
        for generation in self.runtime.generations():
            for key, transport in tuple(generation.pools.items()):
                if key[0] is self:
                    await self.runtime.close_transport(transport)
                    generation.pools.pop(key, None)
        self.closed = True


class ManagedAsyncClient(httpx.AsyncClient):
    def __init__(
        self, *, runtime=None, proxy=None, proxies=None, use_proxy=True, **kwargs
    ):
        self.proxy_runtime = runtime or proxy_runtime
        if kwargs.pop("mounts", None):
            raise ProxyPolicyError("proxy_custom_mounts_unmanaged")
        kwargs.pop("trust_env", None)
        options = {
            key: kwargs[key]
            for key in ("verify", "cert", "http1", "http2", "limits")
            if key in kwargs
        }
        routing = RoutingTransport(
            runtime=self.proxy_runtime,
            explicit=proxies or proxy,
            use_proxy=use_proxy,
            options=options,
            custom=kwargs.pop("transport", None),
        )
        super().__init__(transport=routing, trust_env=False, **kwargs)

    async def send(self, request, **kwargs):
        async with self.proxy_runtime.request_scope():
            return await super().send(request, **kwargs)


async def probe_proxy(policy: ProxyPolicy) -> dict:
    from zhenxun.services.lifecycle.deadline import shutdown_budget

    with shutdown_budget(10):
        return await _probe_proxy_scoped(policy)


async def _probe_proxy_scoped(policy: ProxyPolicy) -> dict:
    from zhenxun.services.lifecycle.deadline import remaining_timeout

    if not policy.proxy:
        raise ProxyPolicyError("proxy_address_required")
    if proxy_runtime.stopping:
        raise ProxyPolicyError("proxy_runtime_stopping")
    for previous in tuple(proxy_runtime.probes):
        if previous.stopping:
            try:
                await previous.shutdown(remaining_timeout(1))
            except Exception:
                continue
            proxy_runtime.probes.discard(previous)
    if len(proxy_runtime.probes) >= 4:
        raise ProxyPolicyError("proxy_probe_busy")
    runtime = ProxyRuntime()
    runtime._kernel = proxy_runtime._kernel
    proxy_runtime.probes.add(runtime)
    runtime.current = PoolGeneration(
        ProxyPolicy(mode="global", proxy=policy.proxy, bypass=())
    )
    started = time.monotonic()
    result = {}
    try:

        async def probe():
            async with ManagedAsyncClient(
                runtime=runtime, timeout=10, follow_redirects=False
            ) as client:
                async with client.stream("GET", PROBE_URL) as response:
                    if response.status_code == 407:
                        raise ProxyPolicyError("proxy_auth_failed")
                    if response.status_code >= 400:
                        raise ProxyPolicyError("proxy_target_rejected")
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size >= 65536:
                            break

        await asyncio.wait_for(probe(), timeout=remaining_timeout(10))
        result = {
            "ok": True,
            "code": "proxy_probe_ok",
            "duration_ms": round((time.monotonic() - started) * 1000),
        }
        return result
    except (httpx.HTTPError, asyncio.TimeoutError, ProxyPolicyError) as error:
        code = (
            error.code
            if isinstance(error, ProxyPolicyError | ProxyRequestError)
            else "proxy_timeout"
            if isinstance(error, httpx.TimeoutException | asyncio.TimeoutError)
            else "proxy_connect_failed"
        )
        result = {
            "ok": False,
            "code": code,
            "stage": "authentication"
            if code == "proxy_auth_failed"
            else "cleanup"
            if code in {"proxy_cleanup_budget_exhausted", "proxy_resources_unreleased"}
            else "target"
            if code == "proxy_target_rejected"
            else "connection",
            "duration_ms": round((time.monotonic() - started) * 1000),
        }
        return result
    finally:
        try:
            await runtime.shutdown(remaining_timeout(5))
        except Exception:
            proxy_runtime.error_code = "proxy_probe_cleanup_unresolved"
            result["cleanup_code"] = "proxy_probe_cleanup_unresolved"
            if result.get("ok"):
                result.update(
                    ok=False, code="proxy_probe_cleanup_unresolved", stage="cleanup"
                )
        else:
            proxy_runtime.probes.discard(runtime)
            if (
                not proxy_runtime.probes
                and proxy_runtime.error_code == "proxy_probe_cleanup_unresolved"
            ):
                proxy_runtime.error_code = None
        result["duration_ms"] = round((time.monotonic() - started) * 1000)
