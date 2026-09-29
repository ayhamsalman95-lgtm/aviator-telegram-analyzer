"""Browser-side SmartFox event ingestion for the Aviator collector.

This module reuses the existing RoundTracker and pre-round cutoff logic, but
accepts already-observed Chrome-side extensionResponse events from a trusted
browser collector. It never receives browser cookies, passwords, or session
storage.
"""
from __future__ import annotations

import time
from typing import Any

from .config import load_config
from .db import Store
from .netlog import RotatingJsonlLog, safe_url
from .pre_round import ObservableEvent, PreRoundBuffer
from .sfs_codec import unwrap_browser_event
from .tracker import RoundTracker


class BrowserIngestor:
    """Process Chrome-side SmartFox extensionResponse snapshots."""

    def __init__(self, cfg=None):
        self.cfg = cfg or load_config()
        self.store = Store.from_config(self.cfg)
        self.netlog = RotatingJsonlLog(
            self.cfg.logs_dir / "network" / "browser_events.jsonl",
            int(self.cfg["network_log_max_bytes"]),
            int(self.cfg["network_log_backups"]),
        )
        self.tracker = RoundTracker(self.store, self.cfg, netlog=self.netlog)
        self.pre_round = PreRoundBuffer(
            max_events=int(self.cfg["pre_round_max_events"]),
            max_age_s=float(self.cfg["pre_round_window_s"]),
        )
        self._last_snapshot_round = None
        self.started_at = time.time()

    @staticmethod
    def _round_id(params: Any):
        if not isinstance(params, dict):
            return None
        raw = params.get("roundId", params.get("round_id"))
        try:
            value = int(raw)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _state_id(params: Any):
        if not isinstance(params, dict):
            return None
        raw = params.get("newStateId", params.get("newStateid"))
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def _capture(self, timestamp: float, command: str, params: Any) -> None:
        self.pre_round.add(
            ObservableEvent(
                timestamp=timestamp,
                source="browser-js",
                command=command,
                round_id=self._round_id(params),
                state_id=self._state_id(params),
                frame_index=None,
                packet_index=None,
                frame_size=None,
                packet_size=None,
                packet_offset=None,
                packet_end=None,
                inter_arrival_ms=None,
                payload=params,
            )
        )

    def _snapshot(self, command: str, params: Any, timestamp: float) -> None:
        if str(command or "").replace("_", "").lower() != "changestate":
            return
        state = self._state_id(params)
        rid = self._round_id(params)
        if state != 1 or rid is None or rid == self._last_snapshot_round:
            return
        snapshot = self.pre_round.snapshot(rid, timestamp, cutoff_state=1)
        self.netlog.write({"kind": "pre_round_snapshot", **snapshot})
        self._last_snapshot_round = rid

    def ingest(self, events: list[dict[str, Any]]) -> dict[str, int]:
        accepted = ignored = handled = 0
        for ev in events:
            if not isinstance(ev, dict):
                ignored += 1
                continue
            item = unwrap_browser_event(ev)
            if item is None:
                ignored += 1
                continue
            command, params = item
            raw_t = ev.get("t")
            try:
                timestamp = float(raw_t) / 1000.0 if raw_t is not None else time.time()
            except (TypeError, ValueError):
                timestamp = time.time()

            self._capture(timestamp, command, params)
            self._snapshot(command, params, timestamp)
            result = self.tracker.handle(command, params, origin="browser-js")
            self.netlog.write({
                "kind": "browser_sfs_event",
                "source": "browser-js",
                "command": command,
                "params": params,
                "event_time": timestamp,
                "tracker_result": result,
            })
            accepted += 1
            if result not in {"ignored", "unhandled"}:
                handled += 1
        return {"accepted": accepted, "handled": handled, "ignored": ignored}
