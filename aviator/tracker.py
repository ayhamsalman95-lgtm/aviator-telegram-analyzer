"""Authoritative round state machine for Spribe Aviator (SmartFox commands).

Commands handled
  changeState       {roundId, newStateId}  1=betting (prediction cutoff), 2=flying, 3=crashed
  roundChartInfo    {roundId, maxMultiplier}  -> the ONLY live completed-round source
  init              {..., roundsInfo:[{roundId, maxMultiplier}, ...]} -> historical backfill
  serverSeedResponse / any command with explicit fairness objects -> fairness evidence

Design guarantees
  * current round id is driven ONLY by changeState (monotonic); a late
    roundChartInfo for the previous round never wipes the new round state.
  * results are keyed by round id (UNIQUE) -> duplicates from the Python decoder
    and the Chrome SmartFox hook collapse into one row.
  * fairness evidence is attached to a round only if its own object carries roundId.
"""
from __future__ import annotations

import gc
import time
from collections import Counter
from typing import Any, Optional

from .extract import extract_fairness, norm
from .predict import make_prediction
from .validation import RoundValidationError, parse_round_id

IGNORED_COMMANDS = {"updatecurrentbets", "updatecurrentcashouts", "updatecurrentcashout",
                    "currentbetsinfo", "onlineplayers", "x", "pingresponse", "betsinfo"}
STATE_NAMES = {1: "betting", 2: "flying", 3: "crashed"}
COMPLETED_QUEUE_CLEANUP_EVERY = 200


def _find_key(data: Any, wanted: str, depth: int = 3) -> Any:
    if depth < 0:
        return None
    if isinstance(data, dict):
        for k, v in data.items():
            if norm(k) == wanted:
                return v
        for v in data.values():
            found = _find_key(v, wanted, depth - 1)
            if found is not None:
                return found
    return None


class RoundTracker:
    def __init__(self, store, cfg, netlog=None, clock=time.time):
        self.store = store
        self.cfg = cfg
        self.netlog = netlog
        self.clock = clock
        self.current_round_id: Optional[int] = None
        self.state_id: Optional[int] = None
        last = store.last_round()
        self.last_completed_id: Optional[int] = int(last["round_id"]) if last else None
        self.counters: Counter = Counter()
        self.last_event_at: float = clock()
        self.completed_queue: list[int] = []
        self._completed_since_cleanup = 0

    # ------------------------------------------------------------ helpers
    def _log(self, kind: str, **fields: Any) -> None:
        if self.netlog is not None:
            self.netlog.write({"kind": kind, **fields})

    @property
    def phase(self) -> str:
        return STATE_NAMES.get(self.state_id, "waiting")

    def snapshot(self) -> dict:
        return {"current_round_id": self.current_round_id, "phase": self.phase,
                "last_completed_id": self.last_completed_id, "counters": dict(self.counters)}

    def _cleanup_completed_queue_if_due(self) -> None:
        if self._completed_since_cleanup < COMPLETED_QUEUE_CLEANUP_EVERY:
            return
        # Fairness only needs to know that at least one completed round exists.
        # Keep the newest id so the enabled autoclick path remains signaled.
        if self.completed_queue:
            self.completed_queue[:] = self.completed_queue[-1:]
        self._completed_since_cleanup = 0
        gc.collect()

    # ------------------------------------------------------------ dispatch
    def handle(self, cmd: Optional[str], params: Any, origin: str = "sfs") -> str:
        self.last_event_at = self.clock()
        n = norm(cmd or "")
        self.counters[f"cmd:{n or 'none'}"] += 1
        if n in IGNORED_COMMANDS or not n:
            return "ignored"
        if n == "roundchartinfo":
            return self._on_round_result(params, origin)
        if n == "changestate":
            return self._on_change_state(params, origin)
        if n == "init":
            result = self._on_init(params, origin)
            self._fairness_from(params, f"{origin}:init")
            return result
        if n == "serverseedresponse":
            return self._on_server_seed_response(params, origin)
        stored = self._fairness_from(params, f"{origin}:{cmd}")
        return f"fairness:{stored}" if stored else "unhandled"

    # --------------------------------------------------------- round result
    def _on_round_result(self, params: Any, origin: str) -> str:
        chart = params if isinstance(params, dict) else {}
        rid_raw = chart.get("roundId", chart.get("round_id"))
        mult_raw = chart.get("maxMultiplier", chart.get("max_multiplier"))
        if rid_raw is None or mult_raw is None:
            self.store.quarantine("roundChartInfo_missing_fields", chart, source="sfs:roundChartInfo",
                                  round_id_raw=rid_raw)
            self._log("sfs_round_result_invalid", keys=sorted(map(str, chart.keys())))
            return "invalid"
        status = self.store.insert_round(rid_raw, mult_raw, source="sfs:roundChartInfo",
                                         origin="live", raw=chart)
        self.counters[f"round:{status}"] += 1
        if status == "inserted":
            rid = int(rid_raw)
            self.last_completed_id = max(rid, self.last_completed_id or 0)
            self.completed_queue.append(rid)
            self._completed_since_cleanup += 1
            self._cleanup_completed_queue_if_due()
            print(f"[SFS] ROUND_RESULT round_id={rid} maxMultiplier={float(mult_raw):.2f}x ({origin})",
                  flush=True)
        # Deliberately NOT touching current_round_id / state here.
        return status

    # ---------------------------------------------------------- state change
    def _on_change_state(self, params: Any, origin: str) -> str:
        data = params if isinstance(params, dict) else {}
        try:
            rid = parse_round_id(data.get("roundId", data.get("round_id")))
        except RoundValidationError:
            self._log("sfs_change_state_no_round", keys=sorted(map(str, data.keys())))
            return "invalid"
        state = data.get("newStateId")
        try:
            state = int(state)
        except (TypeError, ValueError):
            state = None
        if self.current_round_id is not None and rid < self.current_round_id:
            self.counters["state:stale"] += 1
            return "stale"
        self.current_round_id = rid
        self.state_id = state
        self._log("sfs_change_state", round_id=rid, new_state_id=state, origin=origin)
        if state == 1:
            return self._freeze_prediction(rid)
        return f"state:{state}"

    def _freeze_prediction(self, rid: int) -> str:
        if self.store.get_round(rid) is not None:
            self.counters["prediction:skipped_result_known"] += 1
            return "prediction_skipped_result_known"
        if self.store.get_prediction(rid) is not None:
            return "prediction_exists"
        now = self.clock()
        row = make_prediction(self.store, self.cfg, rid, now, trigger="changeState.newStateId=1")
        if row and self.store.insert_prediction(row):
            self.counters["prediction:frozen"] += 1
            return "prediction_frozen"
        return "prediction_not_stored"

    # ------------------------------------------------------------- backfill
    def _on_init(self, params: Any, origin: str) -> str:
        items = _find_key(params, "roundsinfo")
        if not isinstance(items, list):
            self._log("sfs_init_without_roundsInfo",
                      keys=sorted(map(str, params.keys())) if isinstance(params, dict) else [])
            return "init:no_roundsInfo"
        inserted = dup = bad = 0
        for item in items:
            if not isinstance(item, dict) or "roundId" not in item or "maxMultiplier" not in item:
                self.store.quarantine("init_roundsInfo_unrecognized_item", item, source="sfs:init.roundsInfo")
                bad += 1
                continue
            status = self.store.insert_round(item["roundId"], item["maxMultiplier"],
                                             source="sfs:init.roundsInfo", origin="backfill",
                                             raw=item, notify=False)
            if status == "inserted":
                inserted += 1
                self.last_completed_id = max(int(item["roundId"]), self.last_completed_id or 0)
            elif status == "duplicate":
                dup += 1
            else:
                bad += 1
        self.counters["backfill:inserted"] += inserted
        self._log("sfs_init_backfill", inserted=inserted, duplicates=dup, rejected=bad, total=len(items))
        print(f"[SFS] init.roundsInfo backfill: +{inserted} new, {dup} dup, {bad} rejected", flush=True)
        return f"init:{inserted}/{dup}/{bad}"

    # ------------------------------------------------------------- fairness
    def _on_server_seed_response(self, params: Any, origin: str) -> str:
        stored = self._fairness_from(params, f"{origin}:serverSeedResponse")
        if not stored:
            self._log("sfs_fairness_response_empty",
                      keys=sorted(map(str, params.keys())) if isinstance(params, dict) else [],
                      params_type=type(params).__name__)
            return "fairness_empty"
        return f"fairness:{stored}"

    def _fairness_from(self, params: Any, source: str) -> int:
        stored = 0
        for rec in extract_fairness(params):
            explicit = rec.round_id is not None and not rec.is_next_commitment
            assoc = "explicit" if explicit else "unassociated"
            rid = rec.round_id if explicit else None
            ctx = self.current_round_id
            src = f"{source}{rec.path}"
            if rec.server_seed:
                stored += self.store.add_fairness_evidence("server_seed", rec.server_seed, src, rid, assoc, ctx)
            if rec.player_seeds:
                stored += self.store.add_fairness_evidence("player_seeds", rec.player_seeds, src, rid, assoc, ctx)
            if rec.commitment:
                stored += self.store.add_fairness_evidence("commitment_sha256", rec.commitment, src, rid,
                                                           assoc, ctx)
            if rec.round_hash:
                stored += self.store.add_fairness_evidence("round_hash_sha512", rec.round_hash, src, rid, assoc, ctx)
        if stored:
            self.counters["fairness:stored"] += stored
        return stored
