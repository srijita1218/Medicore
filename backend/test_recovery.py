"""
Tests for Checkpoint-Based Recovery.

Run with:
    cd backend
    python3 -m unittest test_recovery -v          # just these
    python3 -m unittest discover -v               # everything (scheduler + recovery)

Every test uses its own throw-away SQLite file, so nothing touches the real
backend/data/medicore.db.
"""

import copy
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from amq_scheduler import (
    AdaptiveMultilevelScheduler, Process, EMERGENCY, INTERACTIVE, BACKGROUND,
    RECOVERY_BOOST_TICKS, MAX_RECOVERY_ATTEMPTS,
)
from checkpoint_recovery import (
    CheckpointStore, RecoveryManager, RecoveryError, recovery_workload, SCHEMA_VERSION,
)
import server


class TempDbCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="medicore-test-")
        self.db = os.path.join(self.dir, "test.db")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def new_manager(self) -> RecoveryManager:
        """A fresh manager on the SAME database file == a backend process restart."""
        return RecoveryManager(CheckpointStore(self.db))

    def manager_with(self, procs, auto_interval=0) -> RecoveryManager:
        """Manager running a custom workload (still the real AMQ scheduler)."""
        m = self.new_manager()
        m.scheduler = AdaptiveMultilevelScheduler(procs)
        m.session_id = "test-session"
        m.auto_interval = auto_interval
        m.store.upsert_session(m.session_id, "custom", 0, auto_interval)
        m.checkpoint(label="initial state", kind="initial")
        return m

    def raw_exec(self, sql, args=()):
        with sqlite3.connect(self.db) as conn:
            conn.execute(sql, args)


def row(status, pid):
    return next(p for p in status["processes"] if p["pid"] == pid)


# --------------------------------------------------------------------------
class TestSchedulerSteppingAndSnapshots(unittest.TestCase):
    """The refactor that made recovery possible must not change scheduling."""

    def test_step_loop_matches_run(self):
        from demo import build_workload
        ran = AdaptiveMultilevelScheduler(build_workload())
        ran.run(max_time=100)
        stepped = AdaptiveMultilevelScheduler(build_workload())
        while stepped.step() not in ("done", "blocked"):
            pass
        stepped._merge_gantt()
        self.assertEqual(stepped.gantt, ran.gantt)
        self.assertEqual(stepped.report(), ran.report())

    def test_snapshot_roundtrip_through_json_is_lossless(self):
        from demo import build_workload
        s = AdaptiveMultilevelScheduler(build_workload())
        for _ in range(17):
            s.step()
        state = json.loads(json.dumps(s.to_state()))
        clone = AdaptiveMultilevelScheduler.from_state(state)
        self.assertEqual(clone.to_state(), s.to_state())

    def test_resume_from_snapshot_equals_uninterrupted_run(self):
        """Checkpoint at every tick of the showcase workload: resuming from any of
        them must reproduce the uninterrupted schedule exactly."""
        from demo import build_workload
        reference = AdaptiveMultilevelScheduler(build_workload())
        reference.run(max_time=100)
        live = AdaptiveMultilevelScheduler(build_workload())
        while True:
            state = json.loads(json.dumps(live.to_state()))
            resumed = AdaptiveMultilevelScheduler.from_state(state)
            resumed.run(max_time=100)
            self.assertEqual(resumed.gantt, reference.gantt, f"diverged when resumed from t={live.time}")
            self.assertEqual(resumed.report(), reference.report())
            if live.step() in ("done", "blocked"):
                break

    def test_from_state_rejects_inconsistent_snapshots(self):
        from demo import build_workload
        s = AdaptiveMultilevelScheduler(build_workload())
        s.step()
        good = s.to_state()
        bad_queue = copy.deepcopy(good)
        bad_queue["queues"]["1"].append("P99")
        bad_remaining = copy.deepcopy(good)
        bad_remaining["processes"][0]["remaining"] = 999
        missing_key = {k: v for k, v in good.items() if k != "gantt"}
        for bad in (bad_queue, bad_remaining, missing_key, {}):
            with self.assertRaises(ValueError):
                AdaptiveMultilevelScheduler.from_state(bad)


# --------------------------------------------------------------------------
class TestCheckpointCreation(TempDbCase):
    def test_checkpoint_captures_full_scheduling_state(self):
        m = self.new_manager()
        m.start_session("hospital")
        m.step(10)
        result = m.checkpoint(label="t10")
        good, skipped = m.store.valid_checkpoints(m.session_id)
        self.assertEqual(skipped, [])
        meta, state = good[0]
        self.assertEqual(meta["id"], result["checkpoint_id"])
        self.assertEqual(meta["tick"], 10)
        self.assertEqual(state["time"], 10)
        live = m.scheduler
        by_pid = {p["pid"]: p for p in state["processes"]}
        for p in live.all_processes:                                  # process table + progress + priorities
            self.assertEqual(by_pid[p.pid]["remaining"], p.remaining)
            self.assertEqual(by_pid[p.pid]["base_priority"], p.base_priority)
            self.assertEqual(by_pid[p.pid]["queue"], p.queue)
            self.assertEqual(by_pid[p.pid]["critical"], p.critical)
        for q in (EMERGENCY, INTERACTIVE, BACKGROUND):                # queue assignments, in order
            self.assertEqual(state["queues"][str(q)], [p.pid for p in live.queues[q]])
        self.assertEqual(state["gantt"], [list(g) for g in live.gantt])

    def test_start_session_writes_initial_checkpoint(self):
        m = self.new_manager()
        status = m.start_session("hospital")
        self.assertEqual(status["checkpoint_count"], 1)
        self.assertEqual(status["latest_valid_checkpoint"]["tick"], 0)
        self.assertEqual(status["latest_valid_checkpoint"]["kind"], "initial")

    def test_auto_checkpoint_interval(self):
        m = self.new_manager()
        m.start_session("hospital", auto_interval=5)
        m.step(12)
        ticks = sorted(c["tick"] for c in m.store.list_checkpoints(m.session_id) if c["kind"] == "auto")
        self.assertEqual(ticks, [5, 10])

    def test_checkpoints_are_trimmed_per_session(self):
        from checkpoint_recovery import MAX_CHECKPOINTS_PER_SESSION
        m = self.new_manager()
        m.start_session("hospital")
        for _ in range(MAX_CHECKPOINTS_PER_SESSION + 5):
            m.checkpoint()
        self.assertEqual(m.store.count_checkpoints(m.session_id), MAX_CHECKPOINTS_PER_SESSION)

    def test_cannot_checkpoint_when_scheduler_is_down(self):
        m = self.new_manager()
        m.start_session("hospital")
        m.crash_scheduler()
        with self.assertRaises(RecoveryError):
            m.checkpoint()


# --------------------------------------------------------------------------
class TestRestoration(TempDbCase):
    def test_restore_resumes_progress_instead_of_resetting(self):
        m = self.new_manager()
        m.start_session("hospital")
        m.step(12)
        before = {p["pid"]: (p["executed"], p["remaining"]) for p in m.status()["processes"]}
        m.checkpoint(label="t12")
        m.step(6)
        m.crash_scheduler()
        status = m.restore()
        self.assertEqual(status["time"], 12)
        after = {p["pid"]: (p["executed"], p["remaining"]) for p in status["processes"]}
        self.assertEqual(after, before)
        self.assertTrue(any(ex > 0 and rem > 0 for ex, rem in after.values()),
                        "scenario must include a partially executed process")

    def test_restore_then_finish_matches_uninterrupted_schedule(self):
        reference = AdaptiveMultilevelScheduler(recovery_workload("hospital"))
        reference.run(max_time=200)

        m = self.new_manager()
        m.start_session("hospital")
        m.step(9)
        m.checkpoint()
        m.step(7)                     # work that will be lost
        m.crash_scheduler()
        m.restore()
        final = m.step(until_done=True)["status"]
        self.assertEqual(final["system_status"], "COMPLETED")
        expected = {r["pid"]: r["completion"] for r in reference.report()[0]}
        got = {p["pid"]: p["completion"] for p in final["processes"]}
        self.assertEqual(got, expected)
        self.assertEqual([tuple(g) for g in final["gantt"]], reference.gantt)

    def test_restore_reports_work_lost_and_recovery_time(self):
        m = self.new_manager()
        m.start_session("hospital")
        m.step(8)
        m.checkpoint()
        m.step(5)                     # t = 13, checkpoint is at 8
        m.crash_scheduler()
        status = m.restore()
        self.assertEqual(status["last_restore"]["work_lost_ticks"], 5)
        self.assertGreaterEqual(status["last_restore"]["recovery_ms"], 0)
        self.assertEqual(status["metrics"]["scheduler_restores"], 1)
        self.assertEqual(status["metrics"]["total_work_lost_ticks"], 5)

    def test_restore_specific_checkpoint(self):
        m = self.new_manager()
        m.start_session("hospital")
        m.step(4); first = m.checkpoint()["checkpoint_id"]
        m.step(4); m.checkpoint()
        status = m.restore(first)
        self.assertEqual(status["time"], 4)

    def test_restore_unknown_checkpoint_is_a_clean_error(self):
        m = self.new_manager()
        m.start_session("hospital")
        with self.assertRaises(RecoveryError) as ctx:
            m.restore(9999)
        self.assertEqual(ctx.exception.status, 404)

    def test_restore_with_no_session_is_a_clean_error(self):
        with self.assertRaises(RecoveryError):
            self.new_manager().restore()


# --------------------------------------------------------------------------
class TestRestartRecovery(TempDbCase):
    """Backend process restart: only the SQLite file survives."""

    def test_checkpoints_survive_backend_restart_and_run_resumes(self):
        m1 = self.new_manager()
        m1.start_session("hospital")
        m1.step(11)
        m1.checkpoint(label="before restart")
        m1.step(4)                                    # progress that dies with the process
        sid = m1.session_id
        del m1

        m2 = self.new_manager()                       # brand-new process, nothing in memory
        status = m2.status()
        self.assertFalse(status["live"])
        self.assertEqual(status["system_status"], "DOWN")
        self.assertEqual(status["session_id"], sid)
        self.assertEqual(status["latest_valid_checkpoint"]["tick"], 11)
        self.assertGreaterEqual(len(status["events"]), 1)        # recovery log persisted too

        restored = m2.restore()
        self.assertEqual(restored["time"], 11)
        self.assertEqual(restored["last_restore"]["work_lost_ticks"], 4)   # known via persisted session row
        done = m2.step(until_done=True)["status"]
        self.assertEqual(done["system_status"], "COMPLETED")
        self.assertEqual(done["counts"]["completed"], done["counts"]["total"])

    def test_restart_with_only_the_initial_checkpoint_restarts_from_tick_zero(self):
        m1 = self.new_manager()
        m1.start_session("hospital")
        m1.step(3)
        m2 = self.new_manager()
        self.assertEqual(m2.restore()["time"], 0)

    def test_new_session_does_not_restore_old_session_checkpoints(self):
        m = self.new_manager()
        m.start_session("hospital"); m.step(10); m.checkpoint()
        old = m.session_id
        m.start_session("hospital")
        self.assertNotEqual(m.session_id, old)
        m.crash_scheduler()
        self.assertEqual(m.restore()["time"], 0)


# --------------------------------------------------------------------------
class TestInvalidCheckpoints(TempDbCase):
    def setUp(self):
        super().setUp()
        self.m = self.new_manager()
        self.m.start_session("hospital")
        self.m.step(5); self.good = self.m.checkpoint(label="good")["checkpoint_id"]
        self.m.step(5)

    def test_corrupted_payload_is_skipped_and_previous_valid_is_used(self):
        newest = self.m.checkpoint(label="will be corrupted")["checkpoint_id"]
        self.raw_exec("UPDATE checkpoints SET payload = replace(payload, '\"time\"', '\"tyme\"') WHERE id=?", (newest,))
        self.m.crash_scheduler()
        status = self.m.restore()
        self.assertEqual(status["last_restore"]["checkpoint_id"], self.good)
        self.assertEqual(status["last_restore"]["skipped_invalid"], 1)
        self.assertTrue(any(e["kind"] == "checkpoint_skipped" for e in status["events"]))
        invalid = [c for c in status["checkpoints"] if not c["valid"]]
        self.assertEqual(len(invalid), 1)
        self.assertIn("checksum", invalid[0]["invalid_reason"])

    def test_interrupted_write_is_detected_and_skipped(self):
        partial = self.m.simulate_interrupted_write()["checkpoint_id"]
        self.m.crash_scheduler()
        status = self.m.restore()
        self.assertNotEqual(status["last_restore"]["checkpoint_id"], partial)
        self.assertEqual(status["last_restore"]["checkpoint_id"], self.good)
        reasons = {c["id"]: c["invalid_reason"] for c in status["checkpoints"] if not c["valid"]}
        self.assertIn("interrupted", reasons[partial])

    def test_checksum_valid_but_inconsistent_state_is_rejected(self):
        state = self.m.scheduler.to_state()
        state["queues"]["1"].append("P404")                           # references a process that does not exist
        bad_id = self.m.store.save(self.m.session_id, state, kind="manual", label="inconsistent")
        meta = next(c for c in self.m.store.list_checkpoints(self.m.session_id) if c["id"] == bad_id)
        self.assertFalse(meta["valid"])
        self.assertIn("inconsistent", meta["invalid_reason"])

    def test_unsupported_schema_version_is_rejected(self):
        newest = self.m.checkpoint()["checkpoint_id"]
        self.raw_exec("UPDATE checkpoints SET schema_version=? WHERE id=?", (SCHEMA_VERSION + 1, newest))
        meta = next(c for c in self.m.store.list_checkpoints(self.m.session_id) if c["id"] == newest)
        self.assertFalse(meta["valid"])
        self.assertIn("schema", meta["invalid_reason"])

    def test_all_checkpoints_invalid_fails_gracefully(self):
        self.raw_exec("UPDATE checkpoints SET payload='{not json'")
        self.m.crash_scheduler()
        with self.assertRaises(RecoveryError) as ctx:
            self.m.restore()
        self.assertEqual(ctx.exception.status, 409)
        self.assertIsNone(self.m.scheduler)                           # still down, nothing half-restored
        status = self.m.status()
        self.assertEqual(status["system_status"], "DOWN")
        self.assertTrue(any(e["kind"] == "restore" and e["level"] == "error" for e in status["events"]))

    def test_cannot_restore_a_specific_invalid_checkpoint(self):
        partial = self.m.simulate_interrupted_write()["checkpoint_id"]
        with self.assertRaises(RecoveryError):
            self.m.restore(partial)


# --------------------------------------------------------------------------
class TestProcessFailureAndRecovery(TempDbCase):
    def two_interactive(self):
        return [Process("P1", "Dash A", INTERACTIVE, 0, 20, 5),
                Process("P2", "Dash B", INTERACTIVE, 0, 20, 5)]

    def test_failure_validation(self):
        m = self.manager_with([Process("P1", "Early", INTERACTIVE, 0, 2, 5),
                               Process("P2", "Later", INTERACTIVE, 50, 5, 5)])
        with self.assertRaises(RecoveryError) as unknown:
            m.fail_process("NOPE")
        self.assertEqual(unknown.exception.status, 404)
        with self.assertRaises(RecoveryError):
            m.fail_process("P2")                                      # not arrived yet
        m.step(ticks=2)                                               # P1 completes
        with self.assertRaises(RecoveryError):
            m.fail_process("P1")                                      # already completed

    def test_cannot_fail_twice_and_recover_requires_a_failure(self):
        m = self.manager_with(self.two_interactive())
        m.step(ticks=3)
        m.fail_process("P1")
        with self.assertRaises(RecoveryError):
            m.fail_process("P1")
        m.recover_failed()
        with self.assertRaises(RecoveryError):
            m.recover_failed()                                        # nothing left to recover

    def test_failed_process_leaves_queues_and_system_degrades(self):
        m = self.manager_with(self.two_interactive())
        m.step(ticks=3)
        status = m.fail_process("P1")
        self.assertEqual(status["system_status"], "DEGRADED")
        self.assertEqual(row(status, "P1")["state"], "FAILED")
        self.assertNotIn("P1", sum(status["queues"].values(), []))
        self.assertEqual(status["counts"]["failed"], 1)

    def test_recovery_restores_checkpointed_progress_and_reports_lost_work(self):
        m = self.manager_with(self.two_interactive())
        m.step(ticks=4)                                               # P1 ran t0-3 (remaining 16)
        m.checkpoint(label="t4")
        m.step(ticks=6)                                               # P2 t4-7, P1 t8-9 -> P1 remaining 14
        self.assertEqual(m.scheduler.get_process("P1").remaining, 14)
        m.fail_process("P1")
        result = m.recover_failed("P1")["results"][0]
        self.assertEqual(result["outcome"], "recovered")
        self.assertEqual(result["remaining"], 16)                     # rolled back to the checkpoint, NOT to burst (20)
        self.assertEqual(result["work_lost_ticks"], 2)
        p1 = m.scheduler.get_process("P1")
        self.assertFalse(p1.failed)
        self.assertTrue(p1.recovered)
        self.assertEqual(p1.queue, INTERACTIVE)                       # normal assignment untouched
        self.assertEqual(m.status()["metrics"]["total_work_lost_ticks"], 2)

    def test_recovered_processes_run_to_completion(self):
        m = self.manager_with(self.two_interactive())
        m.step(ticks=6); m.checkpoint()
        m.step(ticks=4)
        m.fail_process("P2")
        m.recover_failed()
        final = m.step(until_done=True)["status"]
        self.assertEqual(final["system_status"], "COMPLETED")
        self.assertEqual(sum(1 for p in final["processes"] if p["state"] == "COMPLETED"), 2)

    def test_scheduler_is_blocked_not_spinning_while_only_failed_work_remains(self):
        m = self.manager_with([Process("P1", "Only", INTERACTIVE, 0, 10, 5)])
        m.step(ticks=3)
        m.fail_process("P1")
        t = m.scheduler.time
        out = m.step(ticks=5)
        self.assertEqual(out["result"], "blocked")
        self.assertEqual(out["advanced"], 0)
        self.assertEqual(m.scheduler.time, t)                         # clock did not burn idle ticks
        m.recover_failed()
        self.assertEqual(m.step(until_done=True)["status"]["system_status"], "COMPLETED")

    def test_process_that_arrived_after_the_last_checkpoint_restores_to_its_pristine_state(self):
        m = self.manager_with([Process("P1", "Early", INTERACTIVE, 0, 30, 5),
                               Process("P2", "Late", INTERACTIVE, 6, 12, 5)])
        m.step(ticks=10)                       # P2 arrived at t=6; the only checkpoint is the initial t=0 one
        m.fail_process("P2")
        result = m.recover_failed()["results"][0]
        self.assertEqual(result["remaining"], 12)          # nothing newer was saved, so no progress to keep
        self.assertEqual(result["queue"], "INTERACTIVE")   # falls back to its normal queue
        self.assertEqual(result["work_lost_ticks"], m.status()["metrics"]["total_work_lost_ticks"])
        self.assertGreater(result["work_lost_ticks"], 0)

    def test_recovery_with_no_usable_checkpoint_restarts_process_and_warns(self):
        m = self.manager_with([Process("P1", "Only", INTERACTIVE, 0, 12, 5)])
        m.step(ticks=5)
        self.raw_exec("UPDATE checkpoints SET payload='garbage'")          # every checkpoint now invalid
        m.fail_process("P1")
        result = m.recover_failed()["results"][0]
        self.assertEqual(result["outcome"], "recovered")
        self.assertIsNone(result["checkpoint_id"])
        self.assertEqual(result["remaining"], 12)                          # degraded fallback: from scratch
        self.assertTrue(any(e["level"] == "warn" and "no checkpoint" in e["message"].lower()
                            for e in m.status()["events"]))

    def test_recovery_uses_a_checkpoint_where_the_process_was_still_healthy(self):
        """A checkpoint taken *while* a process is failed must not become its restore point."""
        m = self.manager_with(self.two_interactive())
        m.step(ticks=4); m.checkpoint(label="healthy @4")
        m.step(ticks=4)
        m.fail_process("P1")
        m.checkpoint(label="taken while P1 failed")
        result = m.recover_failed("P1")["results"][0]
        self.assertEqual(result["remaining"], 16)

    def test_failure_state_survives_checkpoint_and_restore(self):
        m = self.manager_with(self.two_interactive())
        m.step(ticks=4)
        m.fail_process("P1")
        m.checkpoint(label="degraded")
        m.crash_scheduler()
        status = m.restore()
        self.assertEqual(row(status, "P1")["state"], "FAILED")
        self.assertEqual(status["system_status"], "DEGRADED")


class TestRecoveryAttempts(TempDbCase):
    def setUp(self):
        super().setUp()
        self.m = self.manager_with([Process("P1", "Dash", INTERACTIVE, 0, 20, 5, critical=True)])
        self.m.step(ticks=4)

    def test_failed_recovery_attempts_are_tracked_then_succeed(self):
        self.m.fail_process("P1", recovery_failures=2)
        r1 = self.m.recover_failed("P1")["results"][0]
        r2 = self.m.recover_failed("P1")["results"][0]
        r3 = self.m.recover_failed("P1")["results"][0]
        self.assertEqual([r["outcome"] for r in (r1, r2, r3)], ["attempt_failed", "attempt_failed", "recovered"])
        self.assertEqual(self.m.scheduler.get_process("P1").recovery_attempts, 3)
        self.assertEqual(self.m.status()["metrics"]["failed_recovery_attempts"], 2)

    def test_process_becomes_unrecoverable_after_max_attempts(self):
        self.m.fail_process("P1", recovery_failures=MAX_RECOVERY_ATTEMPTS)
        for _ in range(MAX_RECOVERY_ATTEMPTS):
            self.assertEqual(self.m.recover_failed("P1")["results"][0]["outcome"], "attempt_failed")
        status = self.m.status()
        self.assertEqual(row(status, "P1")["state"], "UNRECOVERABLE")
        self.assertEqual(self.m.recover_failed("P1")["results"][0]["outcome"], "unrecoverable")
        self.assertEqual(self.m.scheduler.get_process("P1").recovery_attempts, MAX_RECOVERY_ATTEMPTS)
        self.assertEqual(self.m.status()["system_status"], "DEGRADED")      # honest: not silently "fixed"

    def test_recovery_failures_input_is_validated(self):
        with self.assertRaises(RecoveryError):
            self.m.fail_process("P1", recovery_failures=99)
        with self.assertRaises(RecoveryError):
            self.m.fail_process("P1", recovery_failures=-1)


# --------------------------------------------------------------------------
class TestRecoveryPriorityAndEmergencySafety(TempDbCase):
    def test_recovery_order_emergency_then_critical_then_rest(self):
        procs = [
            Process("P1", "Billing", BACKGROUND, 0, 20, 9),
            Process("P2", "Doctor Dashboard", INTERACTIVE, 0, 20, 5, critical=True),
            Process("P3", "ECG Alert", EMERGENCY, 0, 30, 1, critical=True),
            Process("P4", "Notes", INTERACTIVE, 0, 20, 5),
        ]
        m = self.manager_with(procs)
        m.step(ticks=2)
        for pid in ("P1", "P2", "P4", "P3"):
            m.fail_process(pid)
        order = [r["pid"] for r in m.recover_failed()["results"]]
        self.assertEqual(order, ["P3", "P2", "P4", "P1"])
        self.assertEqual(order[0], "P3", "genuine emergency process is recovered first")

    def test_flapping_process_does_not_jump_ahead_of_same_tier_peers(self):
        procs = [Process("P1", "A", INTERACTIVE, 0, 20, 5, critical=True),
                 Process("P2", "B", INTERACTIVE, 0, 20, 5, critical=True)]
        m = self.manager_with(procs)
        m.step(ticks=2)
        m.fail_process("P1", recovery_failures=1)
        m.fail_process("P2")
        order1 = [r["pid"] for r in m.recover_failed()["results"]]
        self.assertEqual(order1, ["P1", "P2"])                         # P1 first (same attempts, failed earlier) ...
        self.assertEqual(m.scheduler.get_process("P1").recovery_attempts, 1)
        self.assertTrue(m.scheduler.get_process("P1").failed)

    def test_boost_is_temporary_and_distinct_from_queue_assignment(self):
        procs = [Process("P1", "Critical dash", INTERACTIVE, 0, 40, 5, critical=True),
                 Process("P2", "Other dash", INTERACTIVE, 0, 40, 5)]
        m = self.manager_with(procs)
        m.step(ticks=4); m.checkpoint()
        m.fail_process("P1")
        m.recover_failed("P1")
        p1 = m.scheduler.get_process("P1")
        self.assertEqual(p1.boost_ticks_left, RECOVERY_BOOST_TICKS)
        self.assertEqual(p1.queue, INTERACTIVE)                        # still its normal queue
        self.assertEqual(row(m.status(), "P1")["recovery_boost_ticks"], RECOVERY_BOOST_TICKS)
        m.step(ticks=RECOVERY_BOOST_TICKS)
        self.assertEqual(p1.boost_ticks_left, 0)                       # spent per CPU tick, then gone
        self.assertEqual(p1.queue, INTERACTIVE)

    def test_boosted_process_runs_first_within_its_queue_then_round_robin_resumes(self):
        procs = [Process("P1", "Critical dash", INTERACTIVE, 0, 40, 5, critical=True),
                 Process("P2", "Other A", INTERACTIVE, 0, 40, 5),
                 Process("P3", "Other B", INTERACTIVE, 0, 40, 5)]
        m = self.manager_with(procs)
        m.step(ticks=4); m.checkpoint()            # P1 ran 0-3
        m.step(ticks=4)                            # P2 ran 4-7
        m.fail_process("P1")
        m.recover_failed("P1")
        start = m.scheduler.time
        m.step(ticks=RECOVERY_BOOST_TICKS)
        slices = [s for s in m.scheduler.gantt if s[0] >= start]
        self.assertTrue(all(s[2] == "P1" for s in slices), "boosted process should use its recovery boost first")
        m.step(ticks=8)
        later = {s[2] for s in m.scheduler.gantt if s[0] >= start + RECOVERY_BOOST_TICKS}
        self.assertTrue({"P2", "P3"} & later, "other processes must get the CPU again once the boost expires")

    def test_non_critical_recovered_process_gets_no_boost(self):
        m = self.manager_with([Process("P1", "Report", BACKGROUND, 0, 20, 9)])
        m.step(ticks=3)
        m.fail_process("P1"); m.recover_failed()
        self.assertEqual(m.scheduler.get_process("P1").boost_ticks_left, 0)

    def test_recovery_boost_never_preempts_a_genuine_emergency(self):
        procs = [Process("P1", "Critical dash", INTERACTIVE, 0, 40, 5, critical=True),
                 Process("P2", "Other dash", INTERACTIVE, 0, 40, 5),
                 Process("E1", "Code Blue", EMERGENCY, 9, 3, 0),
                 Process("E2", "ICU alarm", EMERGENCY, 20, 2, 1)]
        m = self.manager_with(procs)
        m.step(ticks=6); m.checkpoint()
        m.fail_process("P1"); m.recover_failed("P1")                   # boosted at t=6 ...
        m.step(until_done=True)
        sched = m.scheduler
        report = {r["pid"]: r for r in sched.report()[0]}
        self.assertEqual(report["E1"]["response"], 0)                  # ... yet emergencies still start instantly
        self.assertEqual(report["E2"]["response"], 0)
        self.assertEqual(next(s for s in sched.gantt if s[0] <= 9 < s[1])[3], "EMERGENCY")

    def test_emergency_always_runs_while_recovered_work_is_boosted(self):
        """Tick-by-tick: whenever an emergency process is ready, the chosen process is EMERGENCY."""
        procs = [Process("P1", "Critical dash", INTERACTIVE, 0, 50, 5, critical=True),
                 Process("P2", "Other dash", INTERACTIVE, 0, 50, 5),
                 Process("P3", "Billing", BACKGROUND, 0, 20, 9, critical=True)] + \
                [Process(f"E{i}", "Alert", EMERGENCY, 8 + i * 5, 2, 1) for i in range(6)]
        m = self.manager_with(procs)
        m.step(ticks=6); m.checkpoint()
        for pid in ("P1", "P3"):
            m.fail_process(pid)
        m.recover_failed()
        sched = m.scheduler
        while True:
            emergency_waiting = bool(sched.queues[EMERGENCY]) or any(
                p.queue == EMERGENCY and p.arrival == sched.time and p.completion_time is None
                for p in sched.all_processes)
            before = len(sched.gantt)
            if sched.step() in ("done", "blocked"):
                break
            if emergency_waiting:
                self.assertEqual(sched.gantt[before][3], "EMERGENCY", f"emergency was bypassed at t={sched.time - 1}")
        for r in sched.report()[0]:
            if r["queue"] == "EMERGENCY":
                self.assertEqual(r["response"], 0)

    def test_boost_cannot_cross_queues_background_still_waits_for_interactive(self):
        procs = [Process("P1", "Interactive", INTERACTIVE, 0, 30, 5),
                 Process("P2", "Critical batch", BACKGROUND, 0, 10, 9, critical=True)]
        m = self.manager_with(procs)
        m.step(ticks=2); m.checkpoint()
        m.fail_process("P2"); m.recover_failed("P2")
        start = m.scheduler.time
        m.step(ticks=8)
        self.assertTrue(all(s[2] == "P1" for s in m.scheduler.gantt if s[0] >= start),
                        "a boosted BACKGROUND process must not overtake a ready INTERACTIVE process")

    def test_recovered_process_remains_subject_to_aging_promotion(self):
        procs = [Process("P1", "Busy", INTERACTIVE, 0, 500, 5),
                 Process("P2", "Batch", BACKGROUND, 0, 30, 9)]
        m = self.manager_with(procs)
        m.step(ticks=5); m.checkpoint()
        m.fail_process("P2"); m.recover_failed("P2")
        m.step(ticks=60)
        self.assertGreaterEqual(m.scheduler.get_process("P2").promotions, 1)


# --------------------------------------------------------------------------
class TestApiLayer(TempDbCase):
    def call(self, m, method, path, body=None):
        return server.handle_api(m, method, path, body or {})

    def test_full_demo_flow_over_the_api_functions(self):
        m = self.new_manager()
        self.assertEqual(self.call(m, "GET", "/api/health")[0], 200)
        code, st = self.call(m, "GET", "/api/recovery/status")
        self.assertEqual((code, st["system_status"]), (200, "NOT_STARTED"))
        code, st = self.call(m, "POST", "/api/recovery/session", {"workload": "hospital", "auto_interval": 0})
        self.assertEqual((code, st["system_status"]), (200, "HEALTHY"))
        self.call(m, "POST", "/api/recovery/step", {"ticks": 8})
        self.assertEqual(self.call(m, "POST", "/api/recovery/checkpoint", {"label": "demo"})[0], 200)
        self.call(m, "POST", "/api/recovery/step", {"ticks": 5})
        code, st = self.call(m, "POST", "/api/recovery/fail-process", {"pid": "P4"})
        self.assertEqual((code, st["system_status"]), (200, "DEGRADED"))
        code, out = self.call(m, "POST", "/api/recovery/recover", {})
        self.assertEqual((code, out["results"][0]["outcome"]), (200, "recovered"))
        self.assertEqual(self.call(m, "POST", "/api/recovery/crash-scheduler")[1]["system_status"], "DOWN")
        code, st = self.call(m, "POST", "/api/recovery/restore", {})
        self.assertEqual(code, 200)
        code, st = self.call(m, "POST", "/api/recovery/step", {"until_done": True})
        self.assertEqual(st["status"]["system_status"], "COMPLETED")

    def test_error_responses(self):
        m = self.new_manager()
        self.assertEqual(self.call(m, "POST", "/api/recovery/step", {"ticks": 1})[0], 409)       # no session
        self.assertEqual(self.call(m, "GET", "/api/nope")[0], 404)
        self.assertEqual(self.call(m, "DELETE", "/api/recovery/status")[0], 405)
        self.call(m, "POST", "/api/recovery/session", {})
        self.assertEqual(self.call(m, "POST", "/api/recovery/step", {"ticks": 0})[0], 400)
        self.assertEqual(self.call(m, "POST", "/api/recovery/step", {"ticks": "abc"})[0], 400)
        self.assertEqual(self.call(m, "POST", "/api/recovery/step", {"ticks": 10**6})[0], 400)
        self.assertEqual(self.call(m, "POST", "/api/recovery/fail-process", {})[0], 400)
        self.assertEqual(self.call(m, "POST", "/api/recovery/fail-process", {"pid": "P99"})[0], 404)
        self.assertEqual(self.call(m, "POST", "/api/recovery/session", {"workload": "bogus"})[0], 400)
        self.assertEqual(self.call(m, "POST", "/api/recovery/session", {"auto_interval": -5})[0], 400)
        self.assertEqual(self.call(m, "POST", "/api/recovery/restore", {"checkpoint_id": 12345})[0], 404)

    def test_reset_clears_storage(self):
        m = self.new_manager()
        self.call(m, "POST", "/api/recovery/session", {})
        code, st = self.call(m, "POST", "/api/recovery/reset")
        self.assertEqual((code, st["system_status"], st["checkpoint_count"]), (200, "NOT_STARTED", 0))


class TestHttpServer(TempDbCase):
    """One real socket round-trip: static frontend + JSON API from a single origin."""

    def setUp(self):
        super().setUp()
        self.httpd, _ = server.make_server("127.0.0.1", 0, self.db)
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.addCleanup(self.httpd.server_close)
        self.addCleanup(self.httpd.shutdown)

    def get(self, path):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
                return r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers

    def post(self, path, body):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_api_and_static_files(self):
        code, body, _ = self.get("/api/health")
        self.assertEqual((code, json.loads(body)["ok"]), (200, True))
        code, body, headers = self.get("/")
        self.assertEqual(code, 200)
        self.assertIn(b"Medicore", body)
        self.assertEqual(self.get("/pages/recovery.html")[0], 200)
        self.assertEqual(self.get("/js/nav.js")[0], 200)
        code, st = self.post("/api/recovery/session", {"workload": "hospital"})
        self.assertEqual((code, st["system_status"]), (200, "HEALTHY"))

    def test_invalid_json_and_path_traversal(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/recovery/step", data=b"{oops",
                                     headers={"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(ctx.exception.code, 400)
        code, _, _ = self.get("/../backend/amq_scheduler.py")
        self.assertIn(code, (403, 404))
        code, _, _ = self.get("/%2e%2e/backend/amq_scheduler.py")
        self.assertIn(code, (403, 404))


if __name__ == "__main__":
    unittest.main(verbosity=2)
