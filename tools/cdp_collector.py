"""Collect Aviator game 52358 from an already-open Chrome and forward events to Railway.

The browser keeps the login/session state. This script connects to the local
Chrome CDP endpoint, decodes client-visible SmartFox WebSocket frames, keeps a
local evidence log, and forwards decoded commands to the Railway browser
collector over HTTPS.
"""
from __future__ import annotations

import asyncio
import os
import time

import aiohttp
from playwright.async_api import async_playwright

from aviator.collector import Collector
from aviator.sfs_codec import SfsDecoder, dependency_report


def pick_page(browser):
    pages = []
    for context in browser.contexts:
        pages.extend(context.pages)
    for page in pages:
        if "game=52358" in (page.url or ""):
            return page
    details = "\n".join(
        f"- {getattr(page, 'url', '')}"
        for page in pages
    )
    raise RuntimeError(
        "No Aviator game=52358 page found in the connected Chrome. "
        f"Visible pages:\n{details or '(none)'}"
    )


async def post_batches(queue: asyncio.Queue, url: str, token: str) -> None:
    endpoint = url.rstrip("/") + "/browser/events"
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print(f"[CDP] forwarding to {endpoint}", flush=True)
        while True:
            batch = []
            item = await queue.get()
            batch.append(item)
            while len(batch) < 500:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            payload = {"events": batch}
            try:
                async with session.post(
                    endpoint,
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                ) as resp:
                    body = await resp.text()
                    if resp.status >= 300:
                        print(f"[CDP] Railway HTTP {resp.status}: {body[:500]}", flush=True)
                    else:
                        print(
                            f"[CDP] Railway accepted {len(batch)} events: {body[:300]}",
                            flush=True,
                        )
            except Exception as exc:
                print(
                    f"[CDP] Railway forward error: {type(exc).__name__}: {exc}",
                    flush=True,
                )
            finally:
                for _ in batch:
                    queue.task_done()


async def main() -> None:
    cdp_url = os.getenv("AVIATOR_CDP_URL", "http://127.0.0.1:9222")
    railway_url = os.getenv(
        "BROWSER_COLLECTOR_URL",
        "https://aviator-telegram-analyzer-production.up.railway.app",
    )
    token = os.getenv("BROWSER_COLLECTOR_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BROWSER_COLLECTOR_TOKEN is required")

    col = Collector()
    decoder = SfsDecoder()
    forward_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    rep = dependency_report()
    print(
        f"[CDP] connecting to {cdp_url}; sfs2x available={rep['available']}",
        flush=True,
    )

    forwarder = asyncio.create_task(post_batches(forward_queue, railway_url, token))

    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(cdp_url)
            page = pick_page(browser)

            def on_ws(ws):
                url = ws.url
                print(f"[CDP] WS {url}", flush=True)
                col.netlog.write({"kind": "ws_open", "url": url})

                def received(payload):
                    try:
                        if isinstance(payload, (bytes, bytearray, memoryview)):
                            data = bytes(payload)
                            col.on_binary_frame(data, url)
                            result = decoder.decode_frame(data)
                            received_at = time.time()
                            for cmd, params in result.commands:
                                if not isinstance(cmd, str):
                                    continue
                                event = {
                                    "t": int(received_at * 1000),
                                    "data": {
                                        "cmd": cmd,
                                        "params": params if params is not None else {},
                                    },
                                }
                                try:
                                    forward_queue.put_nowait(event)
                                except asyncio.QueueFull:
                                    col.netlog.write(
                                        {
                                            "kind": "railway_forward_queue_full",
                                            "command": cmd,
                                        }
                                    )
                        else:
                            col.on_text_frame(str(payload), url)
                    except Exception as exc:
                        col.netlog.write(
                            {
                                "kind": "frame_handler_error",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )

                ws.on("framereceived", received)
                ws.on(
                    "close",
                    lambda *_: col.netlog.write({"kind": "ws_close", "url": url}),
                )

            def on_response(resp):
                try:
                    col.netlog.write(
                        {
                            "kind": "http_response",
                            "url": resp.url,
                            "status": resp.status,
                        }
                    )
                except Exception:
                    pass

            page.on("websocket", on_ws)
            page.on("response", on_response)

            print(f"[CDP] PAGE {page.url}", flush=True)
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)

            col.tracker.last_event_at = time.time()
            print("[CDP] collecting...", flush=True)

            while True:
                if page.is_closed():
                    raise RuntimeError("Aviator page closed")

                for ctx in browser.contexts:
                    for pg in list(ctx.pages):
                        if "game=52358" not in (pg.url or ""):
                            continue
                        for frame in list(pg.frames):
                            try:
                                events = await frame.evaluate(
                                    "() => (window.__aviatorBuf ? "
                                    "window.__aviatorBuf.splice(0, window.__aviatorBuf.length) : [])"
                                )
                            except Exception:
                                continue
                            col.on_browser_events(events)

                col.store.set_status(
                    "collecting",
                    "يجمع النتائج عبر Chrome CDP ويرسلها إلى Railway",
                    **col.tracker.snapshot(),
                )
                if time.time() - col.tracker.last_event_at > 180:
                    raise RuntimeError("No decoded SmartFox command for 180s")
                await asyncio.sleep(0.5)
    finally:
        forwarder.cancel()
        try:
            await forwarder
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
