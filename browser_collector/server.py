"""HTTP/WebApp front end for the Railway-hosted Aviator browser."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from aiohttp import web

from aviator.browser_ingest import BrowserIngestor


MAX_BODY_BYTES = 512 * 1024
TOKEN = os.environ.get("BROWSER_COLLECTOR_TOKEN", "").strip()
WEBAPP_TOKEN = os.environ.get("BROWSER_WEBAPP_TOKEN", "").strip() or TOKEN
INGESTOR = BrowserIngestor()
NOVNC_ROOT = Path("/usr/share/novnc")


def _cors(response: web.StreamResponse) -> web.StreamResponse:
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


def _authorized(request: web.Request) -> bool:
    token = request.headers.get("Authorization", "")
    query_token = request.query.get("token", "")
    return (
        bool(WEBAPP_TOKEN)
        and (
            token == f"Bearer {WEBAPP_TOKEN}"
            or query_token == WEBAPP_TOKEN
        )
    )


async def health(request: web.Request) -> web.Response:
    return _cors(web.json_response({"ok": True, "service": "aviator-railway-runtime"}))


async def options(request: web.Request) -> web.Response:
    return _cors(web.Response(status=204))


async def status(request: web.Request) -> web.Response:
    if not _authorized(request):
        return _cors(web.json_response({"error": "unauthorized"}, status=401))
    last = INGESTOR.store.last_round()
    return _cors(web.json_response({
        "ok": True,
        "round_count": INGESTOR.store.round_count(),
        "last_round": (
            {
                "round_id": int(last["round_id"]),
                "multiplier": float(last["multiplier"]),
                "source": last["source"],
                "origin": last["origin"],
            }
            if last else None
        ),
    }))


async def browser_app(request: web.Request) -> web.Response:
    if not WEBAPP_TOKEN or request.query.get("token") != WEBAPP_TOKEN:
        return web.Response(text="Unauthorized", status=401)

    html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Aviator Railway Browser</title>
<style>
html,body,#screen{margin:0;width:100%;height:100%;background:#111;overflow:hidden}
#bar{position:fixed;z-index:10;left:8px;right:8px;top:8px;padding:7px 10px;
background:rgba(0,0,0,.72);color:#fff;border-radius:8px;font:13px sans-serif}
</style>
</head>
<body>
<div id="bar">Railway browser • 1xBet / Aviator 52358</div>
<div id="screen"></div>
<script type="module">
import RFB from '/novnc/core/rfb.js';
const token = new URLSearchParams(location.search).get('token') || '';
const wsUrl = (location.protocol === 'https:' ? 'wss://' : 'ws://')
  + location.host + '/websockify?token=' + encodeURIComponent(token);
const rfb = new RFB(document.getElementById('screen'), wsUrl);
rfb.viewOnly = false;
rfb.scaleViewport = true;
rfb.resizeSession = false;
rfb.showDotCursor = true;
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def websockify(request: web.Request) -> web.StreamResponse:
    if not WEBAPP_TOKEN or request.query.get("token") != WEBAPP_TOKEN:
        return web.Response(text="Unauthorized", status=401)

    ws = web.WebSocketResponse(protocols=("binary",))
    await ws.prepare(request)

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 5900)
    except Exception as exc:
        await ws.close(message=f"VNC unavailable: {type(exc).__name__}".encode())
        return ws

    async def tcp_to_ws():
        while not ws.closed:
            data = await reader.read(64 * 1024)
            if not data:
                break
            await ws.send_bytes(data)

    async def ws_to_tcp():
        async for msg in ws:
            if msg.type == web.WSMsgType.BINARY:
                writer.write(msg.data)
                await writer.drain()
            elif msg.type == web.WSMsgType.TEXT:
                writer.write(msg.data.encode("latin-1", "ignore"))
                await writer.drain()
            elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.CLOSED, web.WSMsgType.ERROR):
                break

    tasks = [
        asyncio.create_task(tcp_to_ws()),
        asyncio.create_task(ws_to_tcp()),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        if not ws.closed:
            await ws.close()
    return ws


async def ingest(request: web.Request) -> web.Response:
    if not TOKEN:
        return _cors(web.json_response({"error": "BROWSER_COLLECTOR_TOKEN is not configured"}, status=503))

    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {TOKEN}":
        return _cors(web.json_response({"error": "unauthorized"}, status=401))

    if request.content_length and request.content_length > MAX_BODY_BYTES:
        return _cors(web.json_response({"error": "payload too large"}, status=413))

    try:
        payload = await request.json()
    except Exception:
        return _cors(web.json_response({"error": "invalid JSON"}, status=400))

    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return _cors(web.json_response({"error": "expected {events:[...]}"}, status=400))

    if len(events) > 500:
        return _cors(web.json_response({"error": "too many events"}, status=413))

    try:
        result = INGESTOR.ingest(events)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return _cors(web.json_response({
            "error": f"{type(exc).__name__}: {exc}",
        }, status=500))
    return _cors(web.json_response({"ok": True, **result}))


def create_app() -> web.Application:
    app = web.Application(client_max_size=MAX_BODY_BYTES)
    app.router.add_get("/browser/health", health)
    app.router.add_get("/browser/status", status)
    app.router.add_get("/browser/app", browser_app)
    app.router.add_get("/websockify", websockify)
    app.router.add_options("/browser/events", options)
    app.router.add_post("/browser/events", ingest)
    if NOVNC_ROOT.exists():
        app.router.add_static("/novnc", NOVNC_ROOT, show_index=False)
    return app


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    web.run_app(create_app(), host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
