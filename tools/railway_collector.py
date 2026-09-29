"""Run the Aviator collector inside Railway's persistent Chromium session."""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from playwright.async_api import async_playwright

from aviator.collector import Collector, INJECT_JS
from aviator.config import load_config
from aviator.sfs_codec import dependency_report


def game_url() -> str:
    return load_config()["game_url"]


async def main() -> None:
    profile_dir = Path(
        os.environ.get("BROWSER_PROFILE_DIR", "/app/data/chrome_profile")
    )
    profile_dir.mkdir(parents=True, exist_ok=True)

    col = Collector()
    rep = dependency_report()
    print(
        f"[RAILWAY-CDP] starting persistent Chromium; sfs2x={rep['available']}",
        flush=True,
    )

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=False,
            executable_path=os.environ.get("CHROMIUM_EXECUTABLE", "/usr/bin/chromium"),
            viewport={"width": 1440, "height": 900},
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1440,900",
                "--start-maximized",
                "--remote-allow-origins=*",
            ],
        )

        page = context.pages[0] if context.pages else await context.new_page()

        async def attach_page(pg):
            try:
                await pg.add_init_script(INJECT_JS)
            except Exception:
                pass

            def on_ws(ws):
                url = ws.url
                print(f"[RAILWAY-CDP] WS {url}", flush=True)
                col.netlog.write({"kind": "ws_open", "url": url})

                def received(payload):
                    try:
                        if isinstance(payload, (bytes, bytearray, memoryview)):
                            col.on_binary_frame(bytes(payload), url)
                        else:
                            col.on_text_frame(str(payload), url)
                    except Exception as exc:
                        col.netlog.write({
                            "kind": "frame_handler_error",
                            "error": f"{type(exc).__name__}: {exc}",
                        })

                ws.on("framereceived", received)
                ws.on(
                    "close",
                    lambda *_: col.netlog.write({"kind": "ws_close", "url": url}),
                )

            pg.on("websocket", on_ws)
            return pg

        await attach_page(page)

        if "game=52358" not in (page.url or ""):
            print(f"[RAILWAY-CDP] opening {game_url()}", flush=True)
            await page.goto(game_url(), wait_until="domcontentloaded")

        # Re-open the page after the user has an existing persistent session.
        # The user can log in through the Railway noVNC web app; the session stays
        # in BROWSER_PROFILE_DIR across container restarts.
        await page.bring_to_front()
        col.tracker.last_event_at = time.time()

        print(
            "[RAILWAY-CDP] browser ready; waiting for login/game events...",
            flush=True,
        )

        while True:
            active_game_pages = []
            for pg in list(context.pages):
                if pg.is_closed():
                    continue
                if pg not in active_game_pages:
                    try:
                        if "game=52358" in (pg.url or ""):
                            active_game_pages.append(pg)
                    except Exception:
                        pass

            if not active_game_pages:
                pg = await context.new_page()
                await attach_page(pg)
                await pg.goto(game_url(), wait_until="domcontentloaded")

            for pg in list(context.pages):
                if pg.is_closed():
                    continue
                try:
                    for frame in list(pg.frames):
                        events = await frame.evaluate(
                            "() => (window.__aviatorBuf ? "
                            "window.__aviatorBuf.splice(0, window.__aviatorBuf.length) : [])"
                        )
                        col.on_browser_events(events)
                except Exception:
                    continue

            col.store.set_status(
                "collecting",
                "يجمع النتائج من Chromium داخل Railway",
                **col.tracker.snapshot(),
            )

            if time.time() - col.tracker.last_event_at > 180:
                col.store.set_status(
                    "waiting_login",
                    "بانتظار تسجيل الدخول/إعادة اتصال لعبة Aviator عبر متصفح Railway",
                    **col.tracker.snapshot(),
                )
                try:
                    pg = active_game_pages[0] if active_game_pages else context.pages[0]
                    if not pg.is_closed() and "game=52358" not in (pg.url or ""):
                        await pg.goto(game_url(), wait_until="domcontentloaded")
                        col.tracker.last_event_at = time.time()
                except Exception:
                    pass

            await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
