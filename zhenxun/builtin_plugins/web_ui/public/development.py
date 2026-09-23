"""Same-origin, private-network gateway for launcher-managed frontend HMR."""

from __future__ import annotations

import asyncio
import contextlib
from urllib.parse import urlsplit

import aiohttp
from fastapi import APIRouter, Depends, Request, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from starlette.websockets import WebSocketDisconnect
from yarl import URL

from zhenxun.services.webui_dev import PREFIX

from ..security import is_private_scope, require_private_request

_WAITING_PAGE = """<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WebUI 开发服务</title><style>
body{font:16px/1.6 sans-serif;background:#f5f7fa;color:#303133;
margin:8vh auto;padding:24px;max-width:900px}
main{background:white;border:1px solid #e4e7ed;border-radius:12px;padding:28px}
pre{white-space:pre-wrap;overflow-wrap:anywhere;color:#b42318}</style>
<main><h1>WebUI 开发服务</h1><p id="state">正在准备前端…</p>
<pre id="errors"></pre></main>
<script>
async function check(){try{
 const r=await fetch('/__webui_dev__/status',{credentials:'omit',cache:'no-store'});
 const s=await r.json();
 if(s.state==='ready'){location.reload();return}
 const labels={compiling:'正在编译前端，完成后自动打开。',
 error:'前端编译失败，修正源码后会自动恢复。',
 unavailable:'开发服务不可用，请检查 launcher 输出。'};
 document.getElementById('state').textContent=labels[s.state]||labels.unavailable;
 document.getElementById('errors').textContent=(s.errors||[]).join('\\n\\n');
 }catch(e){document.getElementById('state').textContent='管理连接暂时中断，正在重试。'}
 setTimeout(check,1500)}check();</script></html>"""


def same_origin(websocket: WebSocket) -> bool:
    """Validate HMR against the request origin, not raw forwarding headers."""
    try:
        origin = urlsplit(websocket.headers.get("origin", ""))
        page = urlsplit(str(websocket.url))
        scheme = "https" if page.scheme == "wss" else "http"
        return (
            origin.scheme == scheme
            and origin.hostname == page.hostname
            and (origin.port or (443 if scheme == "https" else 80))
            == (page.port or (443 if scheme == "https" else 80))
            and not origin.username
            and not origin.password
            and not origin.path
            and not origin.query
            and not origin.fragment
        )
    except ValueError:
        return False


async def install_development(app, context, endpoint: tuple[str, str]) -> None:
    """Bind development routes and upstream resources to the WebUI lifecycle."""
    base, token = endpoint
    client = aiohttp.ClientSession(
        trust_env=False,
        cookie_jar=aiohttp.DummyCookieJar(),
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=2, sock_read=60),
        headers={"X-Zhenxun-Dev-Token": token},
    )
    context.add_finalizer(client.close)
    router = APIRouter(dependencies=[Depends(require_private_request)])

    async def status_value():
        try:
            async with client.get(
                base + PREFIX + "status",
                timeout=aiohttp.ClientTimeout(total=2),
                allow_redirects=False,
            ) as response:
                if response.status == 200:
                    return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            pass
        return {"state": "unavailable", "errors": []}

    @router.get(PREFIX + "status")
    async def status():
        return JSONResponse(await status_value(), headers={"Cache-Control": "no-store"})

    async def forward(request: Request, path: str):
        # Build from a fixed authority; neither a path nor a query can select a host.
        target = URL(base).with_path(path).with_query(request.url.query)
        headers = {
            key: value
            for key, value in request.headers.items()
            if key.lower()
            in {
                "accept",
                "accept-encoding",
                "if-none-match",
                "if-modified-since",
                "range",
            }
        }
        try:
            upstream = await client.request(
                request.method, target, headers=headers, allow_redirects=False
            )
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return Response("WebUI development server unavailable", status_code=503)
        allowed = {
            "content-type",
            "content-encoding",
            "etag",
            "last-modified",
            "content-range",
            "accept-ranges",
        }
        response_headers = {
            k: v for k, v in upstream.headers.items() if k.lower() in allowed
        }
        response_headers["Cache-Control"] = "no-store"

        async def stream():
            try:
                async for chunk in upstream.content.iter_chunked(65536):
                    yield chunk
            finally:
                upstream.close()

        return StreamingResponse(
            stream(), status_code=upstream.status, headers=response_headers
        )

    @router.api_route("/", methods=["GET", "HEAD"])
    async def index(request: Request):
        state = await status_value()
        if state.get("state") != "ready":
            return HTMLResponse(
                _WAITING_PAGE, status_code=503, headers={"Cache-Control": "no-store"}
            )
        return await forward(request, PREFIX + "index.html")

    @router.api_route(PREFIX + "{path:path}", methods=["GET", "HEAD"])
    async def assets(request: Request, path: str):
        if any(part in {".", ".."} for part in path.replace("\\", "/").split("/")):
            return Response(status_code=404)
        return await forward(request, PREFIX + path)

    app.include_router(router)

    @app.websocket(PREFIX + "ws")
    async def hmr(websocket: WebSocket):
        if not is_private_scope(websocket.scope) or not same_origin(websocket):
            await websocket.close(code=1008)
            return
        try:
            upstream = await client.ws_connect(
                base + PREFIX + "ws",
                origin=base,
                autoclose=True,
                max_msg_size=16 * 1024 * 1024,
            )
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await websocket.close(code=1013)
            return
        async with upstream:
            await websocket.accept()

            async def receive_browser():
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return
                    if "text" in message:
                        await upstream.send_str(message["text"])
                    elif "bytes" in message:
                        await upstream.send_bytes(message["bytes"])

            async def receive_compiler():
                async for message in upstream:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        await websocket.send_text(message.data)
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        await websocket.send_bytes(message.data)
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        return

            tasks = [
                context.spawn_task(
                    receive_browser(), name="webui-hmr-browser", persistent=False
                ),
                context.spawn_task(
                    receive_compiler(), name="webui-hmr-compiler", persistent=False
                ),
            ]
            try:
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                with contextlib.suppress(WebSocketDisconnect, RuntimeError, OSError):
                    await websocket.close(code=1012)
