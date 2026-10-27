"""
Checkpoint-Based Recovery for the Adaptive Multilevel Queue (AMQ) scheduler.

IMPORTANT -- WHAT THIS IS (AND IS NOT)
--------------------------------------
Everything here is a *simulation* of failure and recovery at the level of the
AMQ scheduler model. "Process failures" and "scheduler restarts" are events
inside the simulator; checkpoints are rows in a SQLite file. Nothing here
restores a real operating-system process, kernel state, or hardware. It
demonstrates the *idea* of checkpoint/restart scheduling on top of the same
``AdaptiveMultilevelScheduler`` used everywhere else in Medicore -- there is no
second scheduler.

Design in one paragraph
-----------------------
``CheckpointStore`` persists full scheduler snapshots (``to_state()``) in SQLite
with a SHA-256 checksum, so they survive backend restarts and corrupt/partial
rows can be detected and skipped. ``RecoveryManager`` owns the live scheduler
for the demo session and exposes the operations the UI needs: step, checkpoint,
fail a process, crash/restart the scheduler, restore, and recover failed
processes. Normal queue assignment (``Process.queue``) is never modified by
recovery; the only thing recovery adds is a *temporary, bounded* boost
(``Process.boost_ticks_left``) that reorders work *within* a queue and can never
let a non-emergency process overtake a genuine EMERGENCY process.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time as _time
import uuid
from contextlib import closing
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from amq_scheduler import (
    AdaptiveMultilevelScheduler, Process, EMERGENCY, INTERACTIVE, BACKGROUND,
    QUEUE_NAMES, RECOVERY_BOOST_TICKS, MAX_RECOVERY_ATTEMPTS,
)

SCHEMA_VERSION = 1
MAX_CHECKPOINTS_PER_SESSION = 100
MAX_STEP_TICKS = 1000
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "medicore.db")

SIMULATION_NOTICE = (
    "Simulated recovery: failures, restarts and checkpoints model the AMQ scheduler only. "
    "No real operating-system process, kernel or hardware state is saved or restored."
)


class RecoveryError(Exception):
    """A recovery operation could not be completed. ``status`` is the suggested HTTP code."""

    def __init__(self, message: str, status: int = 409):
        super().__init__(message)
        self.message = message
        self.status = status


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _digest(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Workloads (reuse the project's existing workload definitions)
# --------------------------------------------------------------------------

def recovery_workload(kind: str = "hospital", seed: int = 1) -> List[Process]:
    """Workload for the recovery demo, built from the existing demo/generator modules.

    ``critical`` marks clinical services that deserve the temporary recovery boost:
    every EMERGENCY process, plus the clinician-facing Doctor Dashboard / EMR Lookup
    in the fixed hospital workload. Administrative background work is never critical.
    """
    if kind == "random":
        from workload_generator import generate_workload
        procs = generate_workload(int(seed))
        for p in procs:
            p.critical = p.queue in (EMERGENCY, INTERACTIVE)
        return procs
    if kind != "hospital":
        raise RecoveryError(f"unknown workload {kind!r}", 400)
    from demo import build_workload
    procs = build_workload()
    critical_pids = {"P1", "P2", "P3", "P4", "P5"}
    for p in procs:
        p.critical = p.pid in critical_pids
    return procs


# --------------------------------------------------------------------------
# Persistent checkpoint store (SQLite)
# --------------------------------------------------------------------------

class CheckpointStore:
    """SQLite-backed checkpoints, session metadata and the recovery event log."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("MEDICORE_DB") or DEFAULT_DB_PATH
        if self.db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self._init_schema()

    # ---- plumbing -----------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        try:
            with closing(self._connect()) as conn, conn:
                conn.executescript("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id     TEXT    NOT NULL,
                    created_at     TEXT    NOT NULL,
                    tick           INTEGER NOT NULL,
                    kind           TEXT    NOT NULL,
                    label          TEXT,
                    schema_version INTEGER NOT NULL,
                    n_completed    INTEGER NOT NULL,
                    n_active       INTEGER NOT NULL,
                    n_failed       INTEGER NOT NULL,
                    payload        TEXT    NOT NULL,
                    checksum       TEXT    NOT NULL,
                    status         TEXT    NOT NULL DEFAULT 'committed'
                );
                CREATE INDEX IF NOT EXISTS idx_cp_session ON checkpoints(session_id, id);
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id    TEXT PRIMARY KEY,
                    workload      TEXT,
                    started_at    TEXT NOT NULL,
                    last_tick     INTEGER NOT NULL DEFAULT 0,
                    auto_interval INTEGER NOT NULL DEFAULT 0,
                    updated_at    TEXT
                );
                CREATE TABLE IF NOT EXISTS recovery_events (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts               TEXT NOT NULL,
                    session_id       TEXT,
                    kind             TEXT NOT NULL,
                    level            TEXT NOT NULL,
                    pid              TEXT,
                    tick             INTEGER,
                    checkpoint_id    INTEGER,
                    work_lost_ticks  INTEGER,
                    recovery_ms      REAL,
                    message          TEXT NOT NULL
                );
                """)
        except sqlite3.Error as exc:
            raise RecoveryError(f"checkpoint storage unavailable: {exc}", 503) from exc

    def _run(self, fn):
        try:
            with closing(self._connect()) as conn, conn:
                return fn(conn)
        except sqlite3.Error as exc:
            raise RecoveryError(f"checkpoint storage error: {exc}", 503) from exc

    # ---- checkpoints ----------------------------------------------------------
    @staticmethod
    def _summarise(state: dict) -> Tuple[int, int, int]:
        procs = state["processes"]
        done = sum(1 for p in procs if p["completion_time"] is not None)
        failed = sum(1 for p in procs if p["failed"] and p["completion_time"] is None)
        queued = sum(len(v) for v in state["queues"].values())
        return done, queued, failed

    def save(self, session_id: str, state: dict, kind: str = "manual", label: str = "") -> int:
        """Atomically persist one checkpoint (a single SQLite transaction)."""
        payload = _canonical(state)
        n_done, n_active, n_failed = self._summarise(state)

        def op(conn):
            cur = conn.execute(
                "INSERT INTO checkpoints (session_id, created_at, tick, kind, label, schema_version,"
                " n_completed, n_active, n_failed, payload, checksum, status)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?, 'committed')",
                (session_id, _now_iso(), state["time"], kind, label, SCHEMA_VERSION,
                 n_done, n_active, n_failed, payload, _digest(payload)))
            self._prune(conn, session_id)
            return cur.lastrowid
        return self._run(op)

    def save_interrupted(self, session_id: str, state: dict, label: str = "interrupted write") -> int:
        """TEST/DEMO HOOK: leave behind what a write killed half-way would leave --
        a truncated payload whose checksum belongs to the full payload and whose
        status never reached 'committed'. Restore must detect and skip it."""
        payload = _canonical(state)
        n_done, n_active, n_failed = self._summarise(state)
        cut = payload[: max(1, len(payload) // 2)]

        def op(conn):
            cur = conn.execute(
                "INSERT INTO checkpoints (session_id, created_at, tick, kind, label, schema_version,"
                " n_completed, n_active, n_failed, payload, checksum, status)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?, 'incomplete')",
                (session_id, _now_iso(), state["time"], "interrupted", label, SCHEMA_VERSION,
                 n_done, n_active, n_failed, cut, _digest(payload)))
            return cur.lastrowid
        return self._run(op)

    @staticmethod
    def _prune(conn, session_id: str):
        conn.execute(
            "DELETE FROM checkpoints WHERE session_id=? AND id NOT IN ("
            " SELECT id FROM checkpoints WHERE session_id=? ORDER BY id DESC LIMIT ?)",
            (session_id, session_id, MAX_CHECKPOINTS_PER_SESSION))

    @staticmethod
    def validate_row(row) -> Tuple[Optional[dict], Optional[str]]:
        """Return (state, None) for a usable checkpoint or (None, reason)."""
        if row["status"] != "committed":
            return None, "interrupted write (never committed)"
        payload = row["payload"]
        if _digest(payload) != row["checksum"]:
            return None, "checksum mismatch (corrupted payload)"
        if row["schema_version"] != SCHEMA_VERSION:
            return None, f"unsupported schema version {row['schema_version']}"
        try:
            state = json.loads(payload)
        except (ValueError, TypeError) as exc:
            return None, f"invalid JSON ({exc})"
        try:
            AdaptiveMultilevelScheduler.from_state(state)       # full structural validation
        except ValueError as exc:
            return None, f"inconsistent scheduler state ({exc})"
        if state.get("time") != row["tick"]:
            return None, "tick metadata does not match payload"
        return state, None

    @staticmethod
    def _meta(row, valid: bool, reason: Optional[str]) -> dict:
        return {
            "id": row["id"], "session_id": row["session_id"], "created_at": row["created_at"],
            "tick": row["tick"], "kind": row["kind"], "label": row["label"] or "",
            "completed": row["n_completed"], "active": row["n_active"], "failed": row["n_failed"],
            "valid": valid, "invalid_reason": reason,
            "size_bytes": len(row["payload"]),
        }

    def list_checkpoints(self, session_id: Optional[str] = None, limit: int = 50) -> List[dict]:
        def op(conn):
            if session_id:
                return conn.execute("SELECT * FROM checkpoints WHERE session_id=? ORDER BY id DESC LIMIT ?",
                                    (session_id, limit)).fetchall()
            return conn.execute("SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        out = []
        for row in self._run(op):
            state, reason = self.validate_row(row)
            out.append(self._meta(row, state is not None, reason))
        return out

    def valid_checkpoints(self, session_id: str) -> Tuple[List[Tuple[dict, dict]], List[dict]]:
        """All checkpoints for the session, newest first: ([(meta, state)...] valid, [meta...] skipped)."""
        rows = self._run(lambda c: c.execute(
            "SELECT * FROM checkpoints WHERE session_id=? ORDER BY id DESC", (session_id,)).fetchall())
        good, skipped = [], []
        for row in rows:
            state, reason = self.validate_row(row)
            if state is None:
                skipped.append(self._meta(row, False, reason))
            else:
                good.append((self._meta(row, True, None), state))
        return good, skipped

    def count_checkpoints(self, session_id: Optional[str] = None) -> int:
        def op(conn):
            if session_id:
                return conn.execute("SELECT COUNT(*) FROM checkpoints WHERE session_id=?", (session_id,)).fetchone()[0]
            return conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0]
        return self._run(op)

    # ---- sessions -----------------------------------------------------------
    def upsert_session(self, session_id: str, workload: str, last_tick: int, auto_interval: int):
        self._run(lambda c: c.execute(
            "INSERT INTO sessions (session_id, workload, started_at, last_tick, auto_interval, updated_at)"
            " VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(session_id) DO UPDATE SET last_tick=excluded.last_tick,"
            " auto_interval=excluded.auto_interval, updated_at=excluded.updated_at",
            (session_id, workload, _now_iso(), last_tick, auto_interval, _now_iso())))

    def get_session(self, session_id: str) -> Optional[dict]:
        row = self._run(lambda c: c.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone())
        return dict(row) if row else None

    def latest_session(self) -> Optional[dict]:
        row = self._run(lambda c: c.execute("SELECT * FROM sessions ORDER BY rowid DESC LIMIT 1").fetchone())
        return dict(row) if row else None

    # ---- event log --------------------------------------------------------------
    def log(self, session_id: Optional[str], kind: str, level: str, message: str, pid: Optional[str] = None,
            tick: Optional[int] = None, checkpoint_id: Optional[int] = None,
            work_lost_ticks: Optional[int] = None, recovery_ms: Optional[float] = None):
        try:
            self._run(lambda c: c.execute(
                "INSERT INTO recovery_events (ts, session_id, kind, level, pid, tick, checkpoint_id,"
                " work_lost_ticks, recovery_ms, message) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (_now_iso(), session_id, kind, level, pid, tick, checkpoint_id,
                 work_lost_ticks, recovery_ms, message)))
        except RecoveryError:
            pass        # never let logging failure mask the real operation

    def events(self, session_id: Optional[str], limit: int = 100) -> List[dict]:
        def op(conn):
            if session_id:
                return conn.execute("SELECT * FROM recovery_events WHERE session_id=? ORDER BY id DESC LIMIT ?",
                                    (session_id, limit)).fetchall()
            return conn.execute("SELECT * FROM recovery_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in self._run(op)]

    def metrics(self, session_id: Optional[str]) -> dict:
        def op(conn):
            where, args = ("WHERE session_id=?", (session_id,)) if session_id else ("", ())
            q = lambda sql: conn.execute(sql.format(where=where), args).fetchone()
            restores = q("SELECT COUNT(*), AVG(recovery_ms), MAX(recovery_ms) FROM recovery_events {where} "
                         + ("AND" if where else "WHERE") + " kind='restore' AND level='success'")
            proc_ok = q("SELECT COUNT(*), AVG(recovery_ms) FROM recovery_events {where} "
                        + ("AND" if where else "WHERE") + " kind='process_recovery' AND level='success'")
            attempts_failed = q("SELECT COUNT(*) FROM recovery_events {where} "
                                + ("AND" if where else "WHERE") + " kind='process_recovery' AND level='error'")
            lost = q("SELECT COALESCE(SUM(work_lost_ticks),0) FROM recovery_events {where} "
                     + ("AND" if where else "WHERE") + " work_lost_ticks IS NOT NULL")
            failures = q("SELECT COUNT(*) FROM recovery_events {where} "
                         + ("AND" if where else "WHERE") + " kind IN ('process_failure','scheduler_crash')")
            return {
                "scheduler_restores": restores[0] or 0,
                "avg_restore_ms": round(restores[1], 3) if restores[1] is not None else None,
                "max_restore_ms": round(restores[2], 3) if restores[2] is not None else None,
                "process_recoveries": proc_ok[0] or 0,
                "avg_process_recovery_ms": round(proc_ok[1], 3) if proc_ok[1] is not None else None,
                "failed_recovery_attempts": attempts_failed[0] or 0,
                "total_work_lost_ticks": lost[0] or 0,
                "failures_injected": failures[0] or 0,
            }
        return self._run(op)

    def clear(self):
        self._run(lambda c: c.executescript(
            "DELETE FROM checkpoints; DELETE FROM sessions; DELETE FROM recovery_events;"))


# --------------------------------------------------------------------------
# Recovery manager: drives the real AMQ scheduler
# --------------------------------------------------------------------------

class RecoveryManager:
    def __init__(self, store: Optional[CheckpointStore] = None):
        self.store = store or CheckpointStore()
        self._lock = threading.RLock()
        self.scheduler: Optional[AdaptiveMultilevelScheduler] = None
        self.session_id: Optional[str] = None
        self.workload = "hospital"
        self.auto_interval = 0
        self.crash_info: Optional[dict] = None       # set while the simulated scheduler is down
        self.last_restore: Optional[dict] = None

    # ---- helpers ------------------------------------------------------------------
    def _log(self, kind, level, message, **kw):
        self.store.log(self.session_id, kind, level, message, **kw)

    def _require_live(self) -> AdaptiveMultilevelScheduler:
        if self.scheduler is None:
            raise RecoveryError("The scheduler is down. Restore a checkpoint (or start a new session) first.", 409)
        return self.scheduler

    def _persist_progress(self):
        if self.scheduler is not None and self.session_id:
            self.store.upsert_session(self.session_id, self.workload, self.scheduler.time, self.auto_interval)

    def _scope_session(self) -> str:
        """Session whose checkpoints are in scope: the live one, or the most recent stored one."""
        if self.session_id:
            return self.session_id
        latest = self.store.latest_session()
        if latest is None:
            raise RecoveryError("No session or checkpoint exists yet. Start a session first.", 404)
        return latest["session_id"]

    # ---- session lifecycle ------------------------------------------------------
    def start_session(self, workload: str = "hospital", seed: int = 1, auto_interval: int = 0) -> dict:
        with self._lock:
            auto_interval = int(auto_interval)
            if auto_interval < 0 or auto_interval > 500:
                raise RecoveryError("auto_interval must be between 0 and 500 ticks", 400)
            procs = recovery_workload(workload, seed)
            self.scheduler = AdaptiveMultilevelScheduler(procs)
            self.session_id = uuid.uuid4().hex[:12]
            self.workload = workload if workload == "hospital" else f"random(seed={int(seed)})"
            self.auto_interval = auto_interval
            self.crash_info = None
            self.last_restore = None
            self.store.upsert_session(self.session_id, self.workload, 0, auto_interval)
            self._log("session", "info", f"Started session '{self.workload}' with {len(procs)} processes "
                                         f"(auto-checkpoint every {auto_interval or 'n/a'} ticks).", tick=0)
            self.checkpoint(label="initial state", kind="initial")
            return self.status()

    def reset_storage(self) -> dict:
        with self._lock:
            self.store.clear()
            self.scheduler = None
            self.session_id = None
            self.crash_info = None
            self.last_restore = None
            return self.status()

    # ---- running ------------------------------------------------------------------
    def step(self, ticks: int = 1, until_done: bool = False) -> dict:
        with self._lock:
            sched = self._require_live()
            ticks = int(ticks)
            if not until_done and not (1 <= ticks <= MAX_STEP_TICKS):
                raise RecoveryError(f"ticks must be between 1 and {MAX_STEP_TICKS}", 400)
            budget = MAX_STEP_TICKS if until_done else ticks
            advanced, last = 0, "ran"
            for _ in range(budget):
                last = sched.step()
                if last in ("done", "blocked"):
                    break
                advanced += 1
                if self.auto_interval and sched.time % self.auto_interval == 0:
                    self.checkpoint(label=f"auto @ t={sched.time}", kind="auto")
            self._persist_progress()
            if last == "blocked":
                self._log("scheduler", "warn", "Scheduler is waiting: only failed processes remain. "
                                               "Run recovery to continue.", tick=sched.time)
            elif last == "done":
                self._log("scheduler", "info", "All processes completed.", tick=sched.time)
            return {"advanced": advanced, "result": last, "status": self.status()}

    # ---- checkpoints ------------------------------------------------------------------
    def checkpoint(self, label: str = "", kind: str = "manual") -> dict:
        with self._lock:
            sched = self._require_live()
            cp_id = self.store.save(self.session_id, sched.to_state(), kind=kind, label=label or kind)
            self._log("checkpoint", "info", f"Checkpoint #{cp_id} saved at t={sched.time} ({kind}).",
                      tick=sched.time, checkpoint_id=cp_id)
            return {"checkpoint_id": cp_id, "tick": sched.time}

    def simulate_interrupted_write(self) -> dict:
        with self._lock:
            sched = self._require_live()
            cp_id = self.store.save_interrupted(self.session_id, sched.to_state())
            self._log("checkpoint", "warn",
                      f"Simulated interrupted write: partial checkpoint #{cp_id} left in storage "
                      "(will be detected and skipped on restore).", tick=sched.time, checkpoint_id=cp_id)
            return {"checkpoint_id": cp_id, "tick": sched.time}

    # ---- failure simulation -------------------------------------------------------------
    def fail_process(self, pid: str, recovery_failures: int = 0) -> dict:
        with self._lock:
            sched = self._require_live()
            recovery_failures = int(recovery_failures)
            if not (0 <= recovery_failures <= MAX_RECOVERY_ATTEMPTS):
                raise RecoveryError(f"recovery_failures must be between 0 and {MAX_RECOVERY_ATTEMPTS}", 400)
            try:
                proc = sched.fail_process(pid, recovery_failures)
            except KeyError as exc:
                raise RecoveryError(str(exc.args[0]), 404) from exc
            except ValueError as exc:
                raise RecoveryError(str(exc), 409) from exc
            extra = (f" ({recovery_failures} upcoming recovery attempt(s) are set to fail)"
                     if recovery_failures else "")
            self._log("process_failure", "error",
                      f"Simulated crash of {pid} ({proc.name}) at t={sched.time}; "
                      f"{proc.burst - proc.remaining}/{proc.burst} ticks executed.{extra}",
                      pid=pid, tick=sched.time)
            self._persist_progress()
            return self.status()

    def crash_scheduler(self) -> dict:
        """Simulate a scheduler crash / restart: all in-memory scheduler state is discarded."""
        with self._lock:
            sched = self._require_live()
            self.crash_info = {"tick": sched.time, "session_id": self.session_id}
            self._persist_progress()
            self._log("scheduler_crash", "error",
                      f"Simulated scheduler crash at t={sched.time}. In-memory queues and process table lost.",
                      tick=sched.time)
            self.scheduler = None
            return self.status()

    # ---- restore ------------------------------------------------------------------------
    def restore(self, checkpoint_id: Optional[int] = None) -> dict:
        """Restore the scheduler from the latest valid checkpoint (or a specific valid one)."""
        with self._lock:
            t0 = _time.perf_counter()
            session = self._scope_session()
            good, skipped = self.store.valid_checkpoints(session)
            for meta in skipped:
                self.store.log(session, "checkpoint_skipped", "warn",
                               f"Skipped checkpoint #{meta['id']} (t={meta['tick']}): {meta['invalid_reason']}.",
                               tick=meta["tick"], checkpoint_id=meta["id"])
            if checkpoint_id is not None:
                good = [(m, s) for (m, s) in good if m["id"] == checkpoint_id]
                if not good:
                    self.store.log(session, "restore", "error",
                                   f"Checkpoint #{checkpoint_id} is missing or invalid; nothing restored.")
                    raise RecoveryError(f"Checkpoint #{checkpoint_id} does not exist or is not valid.", 404)
            if not good:
                self.store.log(session, "restore", "error",
                               "Restore failed: no valid checkpoint available. Start a new session (cold start).")
                raise RecoveryError("No valid checkpoint is available to restore from.", 409)
            meta, state = good[0]
            try:
                restored = AdaptiveMultilevelScheduler.from_state(state)
            except ValueError as exc:       # validated already; defensive
                raise RecoveryError(f"Checkpoint #{meta['id']} could not be applied: {exc}", 409) from exc

            known_tick = (self.crash_info or {}).get("tick") if self.scheduler is None else self.scheduler.time
            if known_tick is None:
                srow = self.store.get_session(session)
                known_tick = srow["last_tick"] if srow else None
            lost = max(0, known_tick - meta["tick"]) if known_tick is not None else None

            self.scheduler = restored
            self.session_id = session
            srow = self.store.get_session(session)
            self.workload = srow["workload"] if srow else self.workload
            self.auto_interval = srow["auto_interval"] if srow else self.auto_interval
            self.crash_info = None
            ms = round((_time.perf_counter() - t0) * 1000, 3)
            self.last_restore = {"checkpoint_id": meta["id"], "tick": meta["tick"], "work_lost_ticks": lost,
                                 "recovery_ms": ms, "skipped_invalid": len(skipped)}
            lost_txt = f"{lost} ticks of simulated work lost" if lost is not None else "work lost unknown"
            self._log("restore", "success",
                      f"Restored checkpoint #{meta['id']} (t={meta['tick']}) in {ms} ms; {lost_txt}; "
                      f"{len(skipped)} invalid checkpoint(s) skipped. Incomplete processes keep their progress.",
                      tick=meta["tick"], checkpoint_id=meta["id"], work_lost_ticks=lost, recovery_ms=ms)
            self._persist_progress()
            return self.status()

    # ---- recovery of individual failed processes ------------------------------------------
    @staticmethod
    def recovery_order_key(proc: Process):
        """Who gets recovered first. Tier 0 = genuine EMERGENCY processes, tier 1 = other
        critical services, tier 2 = everything else. Within a tier: clinical severity,
        fewer previous attempts (so a flapping process cannot monopolise recovery), then
        whoever has been down longest."""
        tier = 0 if proc.queue == EMERGENCY else (1 if proc.critical else 2)
        return (tier, proc.base_priority, proc.recovery_attempts, proc.failed_at or 0)

    def _healthy_copy(self, pid: str) -> Tuple[Optional[dict], Optional[dict], List[dict]]:
        """Newest valid checkpoint holding a *non-failed* copy of ``pid`` -> (meta, state, skipped)."""
        good, skipped = self.store.valid_checkpoints(self.session_id)
        for meta, state in good:
            for pd in state["processes"]:
                if pd["pid"] == pid and not pd["failed"] and pd["completion_time"] is None:
                    return meta, state, skipped
        return None, None, skipped

    def recover_failed(self, pid: Optional[str] = None) -> dict:
        with self._lock:
            sched = self._require_live()
            failed = [p for p in sched.all_processes if p.failed]
            if pid is not None:
                failed = [p for p in failed if p.pid == pid]
            if not failed:
                raise RecoveryError("No failed process to recover." if pid is None
                                    else f"{pid} is not in the failed state.", 409)
            failed.sort(key=self.recovery_order_key)
            results = []
            for proc in failed:
                results.append(self._recover_one(sched, proc))
            if any(r["outcome"] == "recovered" for r in results):
                self.checkpoint(label="post-recovery", kind="post-recovery")
            self._persist_progress()
            return {"results": results, "status": self.status()}

    def _recover_one(self, sched: AdaptiveMultilevelScheduler, proc: Process) -> dict:
        t0 = _time.perf_counter()
        pid = proc.pid
        if proc.recovery_attempts >= MAX_RECOVERY_ATTEMPTS:
            self._log("process_recovery", "error",
                      f"{pid} ({proc.name}) is unrecoverable: {MAX_RECOVERY_ATTEMPTS} attempts exhausted.",
                      pid=pid, tick=sched.time)
            return {"pid": pid, "outcome": "unrecoverable", "attempts": proc.recovery_attempts}

        proc.recovery_attempts += 1
        attempt = proc.recovery_attempts

        if proc.recovery_failures_pending > 0:
            proc.recovery_failures_pending -= 1
            left = MAX_RECOVERY_ATTEMPTS - attempt
            tail = "no attempts left -- marked unrecoverable" if left <= 0 else f"{left} attempt(s) left"
            self._log("process_recovery", "error",
                      f"Recovery attempt {attempt}/{MAX_RECOVERY_ATTEMPTS} for {pid} ({proc.name}) failed "
                      f"(simulated); {tail}.", pid=pid, tick=sched.time)
            return {"pid": pid, "outcome": "attempt_failed", "attempts": attempt}

        meta, state, _ = self._healthy_copy(pid)
        if state is None:
            remaining, level, cp_id, source = proc.burst, proc.queue, None, "no checkpoint (restart from scratch)"
            level_name = QUEUE_NAMES[level]
        else:
            pd = next(p for p in state["processes"] if p["pid"] == pid)
            remaining, cp_id = pd["remaining"], meta["id"]
            level = next((int(q) for q, pids in state["queues"].items() if pid in pids), proc.queue)
            level_name = QUEUE_NAMES[level]
            source = f"checkpoint #{cp_id} (t={meta['tick']})"

        lost = max(0, remaining - (proc.remaining_at_failure if proc.remaining_at_failure is not None else remaining))
        down_ticks = sched.time - (proc.failed_at or sched.time)
        boost = RECOVERY_BOOST_TICKS if (proc.critical or proc.queue == EMERGENCY) else 0
        sched.reinstate(proc, remaining=remaining, queue_level=level, boost_ticks=boost)
        proc.remaining_at_failure = None
        ms = round((_time.perf_counter() - t0) * 1000, 3)
        done = proc.burst - remaining
        boost_txt = (f"; temporary recovery boost of {boost} CPU ticks inside {level_name}" if boost
                     else "; normal queue position (not a critical service)")
        warn = "" if state is not None else " WARNING: no checkpoint held this process, so all progress was lost."
        self._log("process_recovery", "success" if state is not None else "warn",
                  f"Recovered {pid} ({proc.name}) from {source}: resumes with {done}/{proc.burst} ticks done "
                  f"in {level_name} (attempt {attempt}); {lost} ticks of simulated work lost, "
                  f"down {down_ticks} ticks{boost_txt}.{warn}",
                  pid=pid, tick=sched.time, checkpoint_id=cp_id, work_lost_ticks=lost, recovery_ms=ms)
        return {"pid": pid, "outcome": "recovered", "attempts": attempt, "remaining": remaining,
                "queue": level_name, "work_lost_ticks": lost, "boost_ticks": boost, "checkpoint_id": cp_id}

    # ---- status for the dashboard -------------------------------------------------------------
    @staticmethod
    def _process_state(sched: AdaptiveMultilevelScheduler, p: Process) -> str:
        if p.completion_time is not None:
            return "COMPLETED"
        if p.failed:
            return "UNRECOVERABLE" if p.recovery_attempts >= MAX_RECOVERY_ATTEMPTS else "FAILED"
        if sched.queue_of(p) is None:
            return "NOT_ARRIVED"
        if sched.gantt and sched.gantt[-1][2] == p.pid and sched.gantt[-1][1] == sched.time:
            return "RUNNING"
        return "READY"

    def status(self) -> dict:
        with self._lock:
            sched = self.scheduler
            scope = self.session_id
            if scope is None:
                latest = self.store.latest_session()
                scope = latest["session_id"] if latest else None
            checkpoints = self.store.list_checkpoints(scope, 30) if scope else []
            latest_valid = next((c for c in checkpoints if c["valid"]), None)

            out = {
                "notice": SIMULATION_NOTICE,
                "session_id": scope, "workload": self.workload, "auto_interval": self.auto_interval,
                "constants": {"max_recovery_attempts": MAX_RECOVERY_ATTEMPTS,
                              "recovery_boost_ticks": RECOVERY_BOOST_TICKS},
                "checkpoints": checkpoints,
                "checkpoint_count": len(checkpoints),
                "latest_valid_checkpoint": latest_valid,
                "events": self.store.events(scope, 60) if scope else [],
                "metrics": self.store.metrics(scope),
                "last_restore": self.last_restore,
                "crash_info": self.crash_info,
                "live": sched is not None,
            }
            if sched is None:
                out["system_status"] = "DOWN" if (scope and checkpoints) else "NOT_STARTED"
                out["time"] = None
                out["processes"], out["queues"], out["gantt"] = [], {}, []
                out["counts"] = {"active": 0, "failed": 0, "recovered": 0, "completed": 0, "total": 0}
                return out

            procs = []
            for p in sorted(sched.all_processes, key=lambda x: int(x.pid[1:]) if x.pid[1:].isdigit() else 0):
                state = self._process_state(sched, p)
                procs.append({
                    "pid": p.pid, "name": p.name, "state": state,
                    "queue": QUEUE_NAMES[p.queue],                       # normal assignment
                    "current_queue": QUEUE_NAMES[sched.queue_of(p)] if sched.queue_of(p) is not None else None,
                    "arrival": p.arrival, "burst": p.burst, "executed": p.burst - p.remaining,
                    "remaining": p.remaining, "base_priority": p.base_priority, "critical": p.critical,
                    "recovered": p.recovered, "recovery_attempts": p.recovery_attempts,
                    "recovery_boost_ticks": p.boost_ticks_left,          # temporary recovery priority
                    "failed_at": p.failed_at, "completion": p.completion_time, "promotions": p.promotions,
                })
            counts = {
                "active": sum(1 for p in procs if p["state"] in ("READY", "RUNNING")),
                "failed": sum(1 for p in procs if p["state"] in ("FAILED", "UNRECOVERABLE")),
                "recovered": sum(1 for p in procs if p["recovered"] and p["state"] not in ("FAILED", "UNRECOVERABLE")),
                "completed": sum(1 for p in procs if p["state"] == "COMPLETED"),
                "total": len(procs),
            }
            if counts["completed"] == counts["total"]:
                system = "COMPLETED"
            elif counts["failed"]:
                system = "DEGRADED"
            else:
                system = "HEALTHY"
            out.update({
                "system_status": system, "time": sched.time, "counts": counts, "processes": procs,
                "queues": {QUEUE_NAMES[q]: [p.pid for p in sched.queues[q]] for q in sched.queues},
                "gantt": self._merged_gantt(sched),
            })
            out["idle_ticks"] = sched.cpu_idle_ticks
            out["utilisation"] = round(100 * (sched.time - sched.cpu_idle_ticks) / sched.time, 2) if sched.time else 0.0
            return out

    @staticmethod
    def _merged_gantt(sched: AdaptiveMultilevelScheduler) -> List[list]:
        merged: List[list] = []
        for s, e, pid, q in sched.gantt:
            if merged and merged[-1][2] == pid and merged[-1][1] == s:
                merged[-1][1] = e
            else:
                merged.append([s, e, pid, q])
        return merged
