"""Chrome/Playwright collector for Spribe Aviator (game 52358).

* Supervisor loop with exponential backoff: any session failure (page closed,
  navigation error, login timeout, stale stream) reconnects instead of exiting.
* Watchdog: no SmartFox command for `watchdog_no_event_s` -> reconnect.
* Every packet of every binary WebSocket frame is decoded with sfs2x-py.
* The Chrome-side SmartFox dispatchEvent hook is a second, independent source;
  both feed the same RoundTracker and duplicates collapse on round id.
* Telegram is NOT used here (outbox only).
* Automatic fairness UI opening is opt-in; verified testing showed opening the settings window adds no new WebSocket request.
"""
from __future__ import annotations

import asyncio
import re
import time

from .config import load_config
from .db import Store
from .netlog import RotatingJsonlLog, safe_url
from .pre_round import ObservableEvent, PreRoundBuffer
from .sfs_codec import SfsDecoder, binary_summary, dependency_report, unwrap_browser_event
from .tracker import RoundTracker

INJECT_JS = r"""
(() => {
  if (window.__aviatorHooked) return;
  window.__aviatorHooked = true;
  window.__aviatorBuf = [];
  const push = (item) => {
    try {
      window.__aviatorBuf.push(item);
      if (window.__aviatorBuf.length > 200) window.__aviatorBuf.splice(0, 100);
    } catch (e) {}
  };
  const seen = new WeakSet();
  const snap = (v, d = 0) => {
    if (d > 6) return null;
    if (v === null || v === undefined) return v;
    const t = typeof v;
    if (t === "string" || t === "number" || t === "boolean") return v;
    if (t === "bigint") return String(v);
    if (t !== "object") return null;
    if (seen.has(v)) return null;
    seen.add(v);
    try {
      if (typeof v.getKeysArray === "function") {
        const o = {};
        for (const k of (v.getKeysArray() || []).slice(0, 200)) { try { o[String(k)] = snap(v.get(k), d + 1); } catch (e) {} }
        return o;
      }
      if (typeof v.size === "function" && typeof v.get === "function") {
        const a = [];
        for (let i = 0; i < Math.min(v.size(), 500); i++) { try { a.push(snap(v.get(i), d + 1)); } catch (e) {} }
        return a;
      }
    } catch (e) {}
    if (Array.isArray(v)) return v.slice(0, 500).map(x => snap(x, d + 1));
    const o = {};
    for (const k of Object.keys(v).slice(0, 200)) {
      if (/^(password|passwd|token|authorization|cookie|secret|session)$/i.test(k)) continue;
      try { o[k] = snap(v[k], d + 1); } catch (e) {}
    }
    return o;
  };
  const hook = () => {
    try {
      const C = window.SFS2X && window.SFS2X.SmartFox;
      if (!C || !C.prototype || typeof C.prototype.dispatchEvent !== "function") return false;
      if (C.prototype.__aviatorHooked) return true;
      const orig = C.prototype.dispatchEvent;
      C.prototype.dispatchEvent = function(evt) {
        try {
          const type = String(evt && evt.type || "");
          if (type === "extensionResponse") push({ t: Date.now(), event_type: type, data: snap(evt) });
        } catch (e) {}
        return orig.apply(this, arguments);
      };
      C.prototype.__aviatorHooked = true;
      return true;
    } catch (e) { return false; }
  };
  hook();
  let tries = 0;
  const timer = setInterval(() => { tries += 1; if (hook() || tries > 240) clearInterval(timer); }, 500);
})();
"""

DRAIN_JS = "() => (window.__aviatorBuf ? window.__aviatorBuf.splice(0, window.__aviatorBuf.length) : [])"


class SessionStale(RuntimeError):
    pass


class Collector:
    def __init__(self, cfg=None):
        self.cfg = cfg or load_config()
        self.store = Store.from_config(self.cfg)
        logs = self.cfg.logs_dir
        self.netlog = RotatingJsonlLog(logs / "network" / "game_network.jsonl",
                                       int(self.cfg["network_log_max_bytes"]), int(self.cfg["network_log_backups"]))
        self.tracker = RoundTracker(self.store, self.cfg, netlog=self.netlog)
        self.decoder = SfsDecoder()
        self.pre_round = PreRoundBuffer(max_events=int(self.cfg["pre_round_max_events"]), max_age_s=float(self.cfg["pre_round_window_s"]))
        self._frame_index = 0
        self._last_frame_received_at = None
        self._last_snapshot_round = None
        self._last_fairness_click = 0.0

    @staticmethod
    def _event_round_id(params):
        if not isinstance(params, dict):
            return None
        raw = params.get("roundId", params.get("round_id"))
        try:
            value = int(raw)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _event_state_id(params):
        if not isinstance(params, dict):
            return None
        raw = params.get("newStateId", params.get("newStateid"))
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def _capture_event(self, *, timestamp, source, command, params,
                       frame_index=None, packet_index=None, frame_size=None,
                       packet_size=None, packet_offset=None, packet_end=None,
                       inter_arrival_ms=None):
        self.pre_round.add(ObservableEvent(
            timestamp=timestamp,
            source=source,
            command=command,
            round_id=self._event_round_id(params),
            state_id=self._event_state_id(params),
            frame_index=frame_index,
            packet_index=packet_index,
            frame_size=frame_size,
            packet_size=packet_size,
            packet_offset=packet_offset,
            packet_end=packet_end,
            inter_arrival_ms=inter_arrival_ms,
            payload=params,
        ))

    def _maybe_snapshot(self, command, params, timestamp):
        if str(command or "").replace("_", "").lower() != "changestate":
            return
        state = self._event_state_id(params)
        rid = self._event_round_id(params)
        if state != 1 or rid is None or rid == self._last_snapshot_round:
            return
        snapshot = self.pre_round.snapshot(rid, timestamp, cutoff_state=1)
        self.netlog.write({"kind": "pre_round_snapshot", **snapshot})
        self._last_snapshot_round = rid

    # --------------------------------------------------------------- frames
    def on_binary_frame(self, data: bytes, ws_url: str) -> None:
        received_at = time.time()
        self._frame_index = getattr(self, "_frame_index", 0) + 1
        frame_index = self._frame_index
        last_frame_received_at = getattr(self, "_last_frame_received_at", None)
        inter_arrival_ms = (
            (received_at - last_frame_received_at) * 1000.0
            if last_frame_received_at is not None else None
        )
        self._last_frame_received_at = received_at
        summary = binary_summary(data)
        self.netlog.write({
            "kind": "ws_binary_frame",
            "url": safe_url(ws_url),
            "frame_index": frame_index,
            "received_at": received_at,
            "inter_arrival_ms": inter_arrival_ms,
            **summary,
        })
        res = self.decoder.decode_frame(data)
        if not res.decoder_available:
            self.netlog.write({"kind": "ws_binary_undecoded", "url": safe_url(ws_url),
                               "frame_index": frame_index, **summary})
            return
        for packet_index, ((cmd, params), span) in enumerate(zip(res.commands, res.packet_spans)):
            self.netlog.write({
                "kind": "sfs_decoded",
                "url": safe_url(ws_url),
                "command": cmd,
                "params": params,
                "frame_index": frame_index,
                "packet_index": packet_index,
                "packet_offset": span["offset"],
                "packet_end": span["end"],
                "packet_size": span["length"],
                "frame_size": len(data),
                "received_at": received_at,
                "inter_arrival_ms": inter_arrival_ms,
            })
            self._capture_event(
                timestamp=received_at, source="py-sfs", command=cmd, params=params,
                frame_index=frame_index, packet_index=packet_index,
                frame_size=len(data), packet_size=span["length"],
                packet_offset=span["offset"], packet_end=span["end"],
                inter_arrival_ms=inter_arrival_ms,
            )
            self._maybe_snapshot(cmd, params, received_at)
            try:
                self.tracker.handle(cmd, params, origin="py-sfs")
            except Exception as exc:
                self.netlog.write({"kind": "tracker_error", "command": cmd, "error": f"{type(exc).__name__}: {exc}"})
        if res.error or res.leftover:
            self.netlog.write({"kind": "sfs_decode_error", "url": safe_url(ws_url), "error": res.error,
                               "packets": res.packets, "leftover": res.leftover,
                               "frame_index": frame_index, **summary})

    def on_text_frame(self, text: str, ws_url: str) -> None:
        if self.cfg["log_text_frames"]:
            self.netlog.write({"kind": "ws_text", "url": safe_url(ws_url), "length": len(text),
                               "payload": text[:20000]})

    def on_browser_events(self, events: list) -> None:
        for ev in events or []:
            unwrapped = unwrap_browser_event(ev)
            if unwrapped is None:
                continue
            cmd, params = unwrapped
            raw_t = ev.get("t") if isinstance(ev, dict) else None
            try:
                timestamp = float(raw_t) / 1000.0 if raw_t is not None else time.time()
            except (TypeError, ValueError):
                timestamp = time.time()
            self._capture_event(timestamp=timestamp, source="js-sfs", command=cmd, params=params)
            self._maybe_snapshot(cmd, params, timestamp)
            try:
                self.tracker.handle(cmd, params, origin="js-sfs")
            except Exception as exc:
                self.netlog.write({"kind": "tracker_error", "command": cmd, "error": f"{type(exc).__name__}: {exc}"})

    # ------------------------------------------------------------- fairness
    async def maybe_open_fairness(self, context) -> None:
        """Disabled by default. When enabled: rate limited, event-driven, no control dumps."""
        if not self.cfg["fairness_autoclick"]:
            return
        if not self.tracker.completed_queue:
            self.netlog.write({
                "kind": "fairness_gate_skip",
                "autoclick": True,
                "queue_len": 0,
            })
            return
        now = time.monotonic()
        if now - self._last_fairness_click < float(self.cfg["fairness_autoclick_min_interval_s"]):
            return
        self._last_fairness_click = now
        settings_re = re.compile(r"provably\s*fair\s*settings", re.I)
        host = self.cfg["aviator_frame_host"]
        for page in list(context.pages):
            for frame in list(page.frames):
                if host not in (frame.url or ""):
                    continue
                try:
                    loc = frame.get_by_text(settings_re)
                    if await loc.count() > 0:
                        visible = []
                        for i in range(await loc.count()):
                            candidate = loc.nth(i)
                            try:
                                if await candidate.is_visible():
                                    visible.append(candidate)
                            except Exception:
                                pass
                        if visible:
                            await visible[0].click(timeout=2500)
                            print("[FAIRNESS] opened Provably Fair Settings (rate limited)", flush=True)
                            self.tracker.completed_queue.clear()
                            return

                    # Diagnostic inventory for icon-only controls. This is read-only and captures
                    # UI metadata only, so we can distinguish the menu icon from the payout dropdown.
                    try:
                        icon_inventory = await frame.evaluate("""() => {
                            const clean = (v) => String(v || "").replace(/\s+/g, " ").trim().slice(0, 180);
                            const visible = (el) => {
                                const r = el.getBoundingClientRect();
                                const cs = getComputedStyle(el);
                                return r.width > 0 && r.height > 0 &&
                                       cs.visibility !== "hidden" && cs.display !== "none" &&
                                       parseFloat(cs.opacity || "1") > 0.05;
                            };
                            const out = [];
                            for (const el of Array.from(document.querySelectorAll("[class*='button-icon' i], [class*='hamb' i], [class*='menu-icon' i]"))) {
                                if (!visible(el)) continue;
                                const r = el.getBoundingClientRect();
                                const cs = getComputedStyle(el);
                                const parent = el.parentElement;
                                const pr = parent ? parent.getBoundingClientRect() : null;
                                const pcs = parent ? getComputedStyle(parent) : null;
                                out.push({
                                    tag: el.tagName.toLowerCase(),
                                    cls: clean(el.className),
                                    x: Math.round(r.x), y: Math.round(r.y),
                                    w: Math.round(r.width), h: Math.round(r.height),
                                    cursor: cs.cursor,
                                    bg: clean(cs.backgroundImage),
                                    content: clean(cs.content),
                                    parent_tag: parent ? parent.tagName.toLowerCase() : "",
                                    parent_cls: parent ? clean(parent.className) : "",
                                    parent_x: pr ? Math.round(pr.x) : null,
                                    parent_y: pr ? Math.round(pr.y) : null,
                                    parent_w: pr ? Math.round(pr.width) : null,
                                    parent_h: pr ? Math.round(pr.height) : null,
                                    parent_cursor: pcs ? pcs.cursor : "",
                                    html: clean(el.outerHTML).slice(0, 900),
                                    parent_html: parent ? clean(parent.outerHTML).slice(0, 1500) : ""
                                });
                            }
                            return out.slice(0, 80);
                        }""")
                        self.netlog.write({
                            "kind": "fairness_button_icon_inventory",
                            "frame_url": safe_url(frame.url),
                            "candidates": icon_inventory,
                        })
                    except Exception as icon_exc:
                        self.netlog.write({
                            "kind": "fairness_button_icon_inventory_error",
                            "frame_url": safe_url(frame.url),
                            "error": str(icon_exc),
                        })

                    # The live DOM identified exactly one .button-icon for the Aviator menu.
                    # Do not rely on viewport coordinates: the browser may scale/reposition the game.
                    # Click its parent dropdown toggle directly, then inspect the opened menu.
                    try:
                        icon_targets = frame.locator("div.dropdown-toggle.button > .button-icon")
                        target_count = await icon_targets.count()
                        clicked = False
                        target_meta = None
                        for i in range(target_count):
                            icon = icon_targets.nth(i)
                            try:
                                if not await icon.is_visible():
                                    continue
                                box = await icon.bounding_box()
                                if not box:
                                    continue
                                bg = await icon.evaluate("(el) => getComputedStyle(el).backgroundImage || ''")
                                target_meta = {
                                    "x": round(box["x"]),
                                    "y": round(box["y"]),
                                    "w": round(box["width"]),
                                    "h": round(box["height"]),
                                    "background_image": "show-more-icon" if "show-more-icon" in bg else bg[:300],
                                }
                                parent = icon.locator("..")
                                await parent.click(timeout=2500)
                                clicked = True
                                await frame.wait_for_timeout(400)
                                self.netlog.write({
                                    "kind": "fairness_menu_exact_click",
                                    "frame_url": safe_url(frame.url),
                                    **target_meta,
                                })
                                break
                            except Exception:
                                continue

                        self.netlog.write({
                            "kind": "fairness_exact_target_probe",
                            "frame_url": safe_url(frame.url),
                            "count": target_count,
                            "target": target_meta,
                            "clicked": clicked,
                        })

                        if clicked:
                            # Capture only visible UI metadata/text after the menu click.
                            try:
                                visible_menu = await frame.evaluate("""() => {
                                    const clean = (v) => String(v || "").replace(/\\s+/g, " ").trim().slice(0, 180);
                                    const visible = (el) => {
                                        const r = el.getBoundingClientRect();
                                        const cs = getComputedStyle(el);
                                        return r.width > 0 && r.height > 0 &&
                                               cs.visibility !== "hidden" && cs.display !== "none" &&
                                               parseFloat(cs.opacity || "1") > 0.05;
                                    };
                                    return Array.from(document.querySelectorAll(
                                        "body *"
                                    )).filter(visible).map(el => {
                                        const r = el.getBoundingClientRect();
                                        const text = clean(el.innerText);
                                        return {
                                            tag: el.tagName.toLowerCase(),
                                            cls: clean(el.className),
                                            text,
                                            aria: clean(el.getAttribute("aria-label")),
                                            title: clean(el.getAttribute("title")),
                                            role: clean(el.getAttribute("role")),
                                            x: Math.round(r.x), y: Math.round(r.y),
                                            w: Math.round(r.width), h: Math.round(r.height)
                                        };
                                    }).filter(x =>
                                        x.text || x.aria || x.title ||
                                        /menu|dropdown|fair|provably|settings/i.test(
                                            (x.cls + " " + x.text + " " + x.aria + " " + x.title)
                                        )
                                    ).sort((a,b) => a.y-b.y || a.x-b.x).slice(-120);
                                }""")
                                self.netlog.write({
                                    "kind": "fairness_menu_after_exact_click",
                                    "frame_url": safe_url(frame.url),
                                    "candidates": visible_menu,
                                })
                            except Exception as menu_probe_exc:
                                self.netlog.write({
                                    "kind": "fairness_menu_after_exact_click_error",
                                    "frame_url": safe_url(frame.url),
                                    "error": str(menu_probe_exc),
                                })

                            loc = frame.get_by_text(settings_re)
                            for j in range(await loc.count()):
                                candidate = loc.nth(j)
                                try:
                                    if await candidate.is_visible():
                                        await candidate.click(timeout=2500)
                                        print("[FAIRNESS] opened Provably Fair Settings via exact Aviator show-more icon", flush=True)
                                        self.tracker.completed_queue.clear()
                                        return
                                except Exception:
                                    continue

                            self.netlog.write({
                                "kind": "fairness_menu_exact_no_settings",
                                "frame_url": safe_url(frame.url),
                            })
                    except Exception as exact_exc:
                        self.netlog.write({
                            "kind": "fairness_menu_exact_error",
                            "frame_url": safe_url(frame.url),
                            "error": str(exact_exc),
                        })

                    # The settings item is normally inside the game's "..." menu.
                    # Open a visible menu/options button first, then retry the item.
                    menu_re = re.compile(r"(more|menu|options|additional)", re.I)
                    for selector in ("button", "[role='button']"):
                        buttons = frame.locator(selector)
                        for i in range(await buttons.count()):
                            button = buttons.nth(i)
                            try:
                                if not await button.is_visible():
                                    continue
                                label = " ".join(filter(None, [
                                    await button.get_attribute("aria-label"),
                                    await button.get_attribute("title"),
                                    await button.inner_text(),
                                ]))
                                if not menu_re.search(label or ""):
                                    continue
                                await button.click(timeout=2500)
                                await frame.wait_for_timeout(300)
                                loc = frame.get_by_text(settings_re)
                                for j in range(await loc.count()):
                                    candidate = loc.nth(j)
                                    try:
                                        if await candidate.is_visible():
                                            await candidate.click(timeout=2500)
                                            print("[FAIRNESS] opened Provably Fair Settings via menu (rate limited)", flush=True)
                                            self.tracker.completed_queue.clear()
                                            return
                                    except Exception:
                                        continue
                            except Exception:
                                continue

                    # Fallback for Aviator's icon-only hamburger menu (three horizontal bars).
                    # Search only visible icon buttons in the upper-right portion of the Aviator frame.
                    try:
                        viewport = await frame.evaluate("""() => ({
                            w: window.innerWidth || document.documentElement.clientWidth || 0,
                            h: window.innerHeight || document.documentElement.clientHeight || 0
                        })""")
                        candidates = frame.locator("button, [role='button']")
                        scored = []
                        for i in range(await candidates.count()):
                            candidate = candidates.nth(i)
                            try:
                                if not await candidate.is_visible():
                                    continue
                                box = await candidate.bounding_box()
                                if not box:
                                    continue
                                if box["x"] < viewport["w"] * 0.70 or box["y"] > viewport["h"] * 0.35:
                                    continue
                                info = await candidate.evaluate("""el => {
                                    const svg = el.querySelector("svg");
                                    if (!svg) return null;
                                    const r = svg.getBoundingClientRect();
                                    const shapes = Array.from(svg.querySelectorAll("line,rect,path,polyline"));
                                    let horizontal = 0;
                                    for (const x of shapes) {
                                      const b = x.getBoundingClientRect();
                                      if (b.width > Math.max(4, b.height * 2.5)) horizontal++;
                                    }
                                    return {
                                      horizontal,
                                      sw: Math.round(r.width),
                                      sh: Math.round(r.height)
                                    };
                                }""")
                                if not info or int(info.get("horizontal") or 0) < 2:
                                    continue
                                # Three horizontal strokes are the strongest signal; tie-break by rightmost.
                                score = (
                                    min(int(info["horizontal"]), 3) * 100000
                                    + int(box["x"]) * 10
                                    - int(box["y"])
                                )
                                scored.append((score, candidate))
                            except Exception:
                                continue
                        scored.sort(key=lambda item: item[0], reverse=True)
                        if scored:
                            await scored[0][1].click(timeout=2500)
                            await frame.wait_for_timeout(300)
                            loc = frame.get_by_text(settings_re)
                            for j in range(await loc.count()):
                                candidate = loc.nth(j)
                                try:
                                    if await candidate.is_visible():
                                        await candidate.click(timeout=2500)
                                        print("[FAIRNESS] opened Provably Fair Settings via hamburger menu (rate limited)", flush=True)
                                        self.tracker.completed_queue.clear()
                                        return
                                except Exception:
                                    continue
                    except Exception as hamburger_exc:
                        self.netlog.write({
                            "kind": "fairness_hamburger_error",
                            "error": str(hamburger_exc),
                        })

                    # Targeted diagnostic for the real three-line (hamburger) menu.
                    # We inspect upper-right hit-test points and their ancestor metadata without clicking them.
                    try:
                        hit_probe = await frame.evaluate("""() => {
                            const clean = (v) => String(v || "").replace(/\\s+/g, " ").trim().slice(0, 120);
                            const w = window.innerWidth || document.documentElement.clientWidth || 0;
                            const h = window.innerHeight || document.documentElement.clientHeight || 0;
                            const points = [
                                [0.90,0.06],[0.94,0.06],[0.97,0.06],
                                [0.90,0.10],[0.94,0.10],[0.97,0.10],
                                [0.90,0.14],[0.94,0.14],[0.97,0.14]
                            ];
                            const seen = new Set();
                            const out = [];
                            for (const [px,py] of points) {
                                const x = Math.max(0, Math.min(w - 1, Math.round(w * px)));
                                const y = Math.max(0, Math.min(h - 1, Math.round(h * py)));
                                let el = document.elementFromPoint(x,y);
                                for (let depth=0; el && depth<4; depth++, el=el.parentElement) {
                                    const key = String(el);
                                    const r = el.getBoundingClientRect();
                                    const rec = {
                                        point:[x,y],
                                        tag:el.tagName.toLowerCase(),
                                        id:clean(el.id),
                                        cls:clean(el.className),
                                        text:clean(el.innerText),
                                        aria:clean(el.getAttribute && el.getAttribute("aria-label")),
                                        title:clean(el.getAttribute && el.getAttribute("title")),
                                        role:clean(el.getAttribute && el.getAttribute("role")),
                                        x:Math.round(r.x), y:Math.round(r.y),
                                        w:Math.round(r.width), h:Math.round(r.height),
                                        html:(el.outerHTML || "").slice(0,500)
                                    };
                                    const sig = JSON.stringify(rec, Object.keys(rec).sort());
                                    if (!seen.has(sig)) { seen.add(sig); out.push(rec); }
                                }
                            }
                            return out.slice(0,80);
                        }""")
                        self.netlog.write({
                            "kind": "fairness_hamburger_hit_probe",
                            "frame_url": safe_url(frame.url),
                            "candidates": hit_probe,
                        })
                    except Exception as hit_exc:
                        self.netlog.write({
                            "kind": "fairness_hamburger_hit_probe_error",
                            "frame_url": safe_url(frame.url),
                            "error": str(hit_exc),
                        })

                    # Targeted menu-element inventory. Do not click here; identify likely
                    # navigation/menu controls by class/tag/icon and geometry.
                    try:
                        menu_inventory = await frame.evaluate("""() => {
                            const clean = (v) => String(v || "").replace(/\\s+/g, " ").trim().slice(0, 160);
                            const visible = (el) => {
                                const r = el.getBoundingClientRect();
                                const cs = getComputedStyle(el);
                                return r.width > 0 && r.height > 0 && cs.visibility !== "hidden" &&
                                       cs.display !== "none" && parseFloat(cs.opacity || "1") > 0.05;
                            };
                            const out = [];
                            const seen = new Set();
                            const selector = [
                                "button","[role='button']","a",
                                "[class*='menu' i]","[class*='hamb' i]",
                                "[class*='option' i]","[class*='setting' i]",
                                "[class*='toolbar' i]","[class*='nav' i]",
                                "[class*='dropdown' i]","svg"
                            ].join(",");
                            for (const el of Array.from(document.querySelectorAll(selector))) {
                                if (!visible(el)) continue;
                                const r = el.getBoundingClientRect();
                                const cs = getComputedStyle(el);
                                const svg = el.tagName.toLowerCase() === "svg" ? el : el.querySelector("svg");
                                const svgText = svg ? clean(svg.outerHTML).slice(0, 900) : "";
                                const hay = [
                                    el.tagName, el.id, el.className,
                                    el.getAttribute("aria-label"), el.getAttribute("title"),
                                    el.getAttribute("data-testid"), el.getAttribute("role")
                                ].map(clean).join(" ").toLowerCase();
                                const iconSignal = /menu|hamb|bars|ellipsis|more|options|settings|nav/.test(hay) ||
                                                    /(<line|<rect|<path|<polyline)/i.test(svgText);
                                const compact = r.width <= 90 && r.height <= 90;
                                const upper = r.y <= 220;
                                const right = r.x >= (window.innerWidth || 0) * 0.65;
                                if (!(iconSignal || (compact && upper && right) || hay.includes("dropdown"))) continue;
                                const rec = {
                                    tag: el.tagName.toLowerCase(),
                                    id: clean(el.id),
                                    cls: clean(el.className),
                                    text: clean(el.innerText),
                                    aria: clean(el.getAttribute("aria-label")),
                                    title: clean(el.getAttribute("title")),
                                    role: clean(el.getAttribute("role")),
                                    cursor: cs.cursor,
                                    position: cs.position,
                                    x: Math.round(r.x), y: Math.round(r.y),
                                    w: Math.round(r.width), h: Math.round(r.height),
                                    html: clean(el.outerHTML).slice(0, 1200)
                                };
                                const key = JSON.stringify(rec);
                                if (!seen.has(key)) { seen.add(key); out.push(rec); }
                            }
                            return out
                              .sort((a,b) => (b.x+b.w)-(a.x+a.w) || a.y-b.y)
                              .slice(0, 120);
                        }""")
                        self.netlog.write({
                            "kind": "fairness_menu_inventory",
                            "frame_url": safe_url(frame.url),
                            "candidates": menu_inventory,
                        })
                    except Exception as inv_exc:
                        self.netlog.write({
                            "kind": "fairness_menu_inventory_error",
                            "frame_url": safe_url(frame.url),
                            "error": str(inv_exc),
                        })

                    # Diagnostic probe: the game menu button can be an icon-only control
                    # with no aria-label/title/text. Record only safe UI metadata, never cookies,
                    # tokens, page source, or arbitrary DOM values.
                    try:
                        probe = await frame.evaluate("""() => {
                            const visible = (el) => {
                                const r = el.getBoundingClientRect();
                                const cs = getComputedStyle(el);
                                return !!(r.width && r.height && cs.visibility !== "hidden" &&
                                           cs.display !== "none" && parseFloat(cs.opacity || "1") > 0.05);
                            };
                            const clean = (v) => String(v || "").replace(/\\s+/g, " ").trim().slice(0, 140);
                            const els = Array.from(document.querySelectorAll(
                                "button,[role='button'],[aria-label],a,[data-testid]"
                            )).filter(visible).map((el) => {
                                const r = el.getBoundingClientRect();
                                return {
                                    tag: el.tagName.toLowerCase(),
                                    text: clean(el.innerText),
                                    aria: clean(el.getAttribute("aria-label")),
                                    title: clean(el.getAttribute("title")),
                                    role: clean(el.getAttribute("role")),
                                    testid: clean(el.getAttribute("data-testid")),
                                    id: clean(el.id),
                                    cls: clean(el.className),
                                    x: Math.round(r.x), y: Math.round(r.y),
                                    w: Math.round(r.width), h: Math.round(r.height)
                                };
                            });
                            return els
                                .sort((a,b) => (b.x + b.w) - (a.x + a.w) || a.y - b.y)
                                .slice(0, 40);
                        }""")
                        self.netlog.write({
                            "kind": "fairness_dom_probe",
                            "frame_url": safe_url(frame.url),
                            "candidates": probe,
                        })
                    except Exception as probe_exc:
                        self.netlog.write({
                            "kind": "fairness_dom_probe_error",
                            "frame_url": safe_url(frame.url),
                            "error": str(probe_exc),
                        })
                    self.netlog.write({"kind": "fairness_menu_not_found"})
                except Exception as exc:
                    self.netlog.write({"kind": "fairness_click_error", "error": str(exc)})
                    return

    # -------------------------------------------------------------- session
    async def _wait_login(self, page) -> None:
        async def needed() -> bool:
            try:
                url = (page.url or "").lower()
                if await page.locator("input[type='password']").count() > 0:
                    return True
                return any(m in url for m in ("/login", "/signin", "/sign-in", "/auth"))
            except Exception:
                return False
        if not await needed():
            return
        self.store.set_status("login_required", "سجّل الدخول يدويًا في نافذة Chrome")
        print("[LOGIN] سجّل الدخول يدويًا في نافذة Chrome المفتوحة.", flush=True)
        deadline = time.monotonic() + float(self.cfg["login_wait_s"])
        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            if not await needed():
                return
        raise RuntimeError("login timeout")

    async def run_session(self) -> None:
        self.pre_round.clear()
        self._last_snapshot_round = None
        self._last_frame_received_at = None
        from playwright.async_api import async_playwright  # imported lazily (optional in tests)
        profile = self.cfg.path("chrome_profile_dir")
        profile.mkdir(parents=True, exist_ok=True)
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(profile), channel="chrome", headless=False,
                viewport={"width": 1440, "height": 900}, args=["--disable-notifications"])
            try:
                await context.add_init_script(INJECT_JS)

                def on_ws(ws):
                    url = ws.url
                    self.netlog.write({"kind": "ws_open", "url": safe_url(url)})

                    def received(payload):
                        try:
                            if isinstance(payload, (bytes, bytearray, memoryview)):
                                self.on_binary_frame(bytes(payload), url)
                            else:
                                self.on_text_frame(str(payload), url)
                        except Exception as exc:
                            self.netlog.write({"kind": "frame_handler_error", "error": str(exc)})
                    ws.on("framereceived", received)
                    ws.on("close", lambda *_: self.netlog.write({"kind": "ws_close", "url": safe_url(url)}))

                def on_response(resp):
                    try:
                        self.netlog.write({"kind": "http_response", "url": safe_url(resp.url),
                                           "status": resp.status})
                    except Exception:
                        pass

                def attach(page):
                    page.on("websocket", on_ws)
                    page.on("response", on_response)

                for pg in context.pages:
                    attach(pg)
                context.on("page", attach)
                page = context.pages[0] if context.pages else await context.new_page()

                self.store.set_status("opening", "فتح الموقع")
                await page.goto(self.cfg["site_home_url"], wait_until="domcontentloaded", timeout=60000)
                await self._wait_login(page)
                self.store.set_status("opening_game", "فتح Aviator 52358")
                await page.goto(self.cfg["game_url"], wait_until="domcontentloaded", timeout=60000)
                if page.url.startswith("chrome-error://"):
                    raise RuntimeError("Chrome could not reach the game page")
                self.store.set_status("collecting", "يجمع النتائج من SmartFox",
                                      decoder=dependency_report())
                self.tracker.last_event_at = time.time()
                watchdog = float(self.cfg["watchdog_no_event_s"])
                while True:
                    if page.is_closed():
                        raise RuntimeError("game page closed")
                    for pg in list(context.pages):
                        for frame in list(pg.frames):
                            try:
                                events = await frame.evaluate(DRAIN_JS)
                            except Exception:
                                continue
                            self.on_browser_events(events)
                    if self.cfg["fairness_autoclick"]:
                        await self.maybe_open_fairness(context)
                    if time.time() - self.tracker.last_event_at > watchdog:
                        raise SessionStale(f"no SmartFox command for {watchdog:.0f}s")
                    self.store.set_status("collecting", "يجمع النتائج", **self.tracker.snapshot())
                    await asyncio.sleep(0.5)
            finally:
                try:
                    await context.close()
                except Exception:
                    pass

    async def supervise(self) -> None:
        backoff = [float(x) for x in self.cfg["reconnect_backoff_s"]] or [10.0]
        failures = 0
        rep = dependency_report()
        print(f"[SFS] sfs2x-py available={rep['available']} {rep.get('error') or ''}", flush=True)
        while True:
            started = time.monotonic()
            try:
                await self.run_session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if time.monotonic() - started > 300:
                    failures = 0  # the session was healthy for a while
                delay = backoff[min(failures, len(backoff) - 1)]
                failures += 1
                self.store.set_status("reconnecting", f"{type(exc).__name__}: {exc}", retry_in_s=delay,
                                      failures=failures)
                print(f"[COLLECTOR] session ended: {type(exc).__name__}: {exc}; reconnect in {delay:.0f}s",
                      flush=True)
                await asyncio.sleep(delay)


def main() -> None:
    col = Collector()
    try:
        asyncio.run(col.supervise())
    except KeyboardInterrupt:
        col.store.set_status("stopped", "stopped by user")
        print("[COLLECTOR] stopped", flush=True)