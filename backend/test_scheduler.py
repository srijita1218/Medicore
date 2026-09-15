"""
Unit tests for the Adaptive Multilevel Queue scheduler.

Run with:
    python3 -m unittest test_scheduler.py -v
or simply:
    python3 test_scheduler.py
"""

import unittest
import copy

import amq_scheduler as sched
from amq_scheduler import (
    Process, AdaptiveMultilevelScheduler, EMERGENCY, INTERACTIVE, BACKGROUND
)
from workload_generator import generate_starvation_stress_workload


class TestEmergencyPreemption(unittest.TestCase):
    def test_emergency_preempts_immediately(self):
        """An Emergency task must start running the instant it arrives,
        even if an Interactive/Background task is mid-execution."""
        procs = [
            Process("P1", "Long Interactive", INTERACTIVE, arrival=0, burst=20, base_priority=5),
            Process("P2", "ICU Alert", EMERGENCY, arrival=5, burst=2, base_priority=1),
        ]
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(procs))
        engine.run(max_time=50)
        rows, _ = engine.report()
        emergency_row = next(r for r in rows if r["pid"] == "P2")
        self.assertEqual(emergency_row["response"], 0,
                          "Emergency task should have zero response time (dispatched instantly).")

    def test_more_severe_emergency_wins_tie(self):
        """Among two ready Emergency tasks, the lower base_priority (more
        clinically severe) one must be dispatched first."""
        procs = [
            Process("P1", "Minor Alert", EMERGENCY, arrival=0, burst=5, base_priority=2),
            Process("P2", "Code Blue", EMERGENCY, arrival=0, burst=5, base_priority=0),
        ]
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(procs))
        engine.run(max_time=20)
        rows, _ = engine.report()
        p1 = next(r for r in rows if r["pid"] == "P1")
        p2 = next(r for r in rows if r["pid"] == "P2")
        self.assertLess(p2["completion"], p1["completion"],
                         "Code Blue (higher severity) should complete before the minor alert.")


class TestRoundRobinFairness(unittest.TestCase):
    def test_interactive_tasks_interleave(self):
        """Two same-priority Interactive tasks longer than one quantum
        must interleave rather than one running to completion first."""
        procs = [
            Process("P1", "Dashboard A", INTERACTIVE, arrival=0, burst=10, base_priority=5),
            Process("P2", "Dashboard B", INTERACTIVE, arrival=0, burst=10, base_priority=5),
        ]
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(procs))
        engine.run(max_time=30)
        pids_in_order = [slot[2] for slot in engine.gantt]
        # If they interleave, P1 should not occupy every early Gantt slice.
        self.assertGreater(len(set(pids_in_order[:4])), 1,
                            "Round Robin should interleave same-priority Interactive tasks.")


class TestFCFSBackground(unittest.TestCase):
    def test_background_runs_only_when_idle(self):
        """A Background task must not preempt or interleave with an
        Interactive task that is still ready."""
        procs = [
            Process("P1", "Backup", BACKGROUND, arrival=0, burst=5, base_priority=9),
            Process("P2", "Dashboard", INTERACTIVE, arrival=0, burst=5, base_priority=5),
        ]
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(procs))
        engine.run(max_time=20)
        rows, _ = engine.report()
        p1 = next(r for r in rows if r["pid"] == "P1")
        p2 = next(r for r in rows if r["pid"] == "P2")
        self.assertLess(p2["completion"], p1["completion"],
                         "Interactive task should finish before the Background task starts meaningfully.")


class TestAgingPreventsStarvation(unittest.TestCase):
    def test_aging_reduces_starvation_rate(self):
        """Across many adversarial seeds, enabling aging must not increase
        (and should typically decrease) the fraction of seeds where a
        Background job fails to complete within the horizon."""
        horizon = 150
        seeds = range(1, 16)

        def starvation_rate(slope_bg):
            original = dict(sched.AGING_SLOPE)
            sched.AGING_SLOPE[BACKGROUND] = slope_bg
            try:
                starved = 0
                for seed in seeds:
                    wl = generate_starvation_stress_workload(seed, horizon=horizon)
                    engine = AdaptiveMultilevelScheduler(copy.deepcopy(wl))
                    engine.run(max_time=horizon)
                    rows, _ = engine.report()
                    if any(r["starved"] for r in rows if r["queue"] == "BACKGROUND"):
                        starved += 1
                return starved / len(list(seeds))
            finally:
                sched.AGING_SLOPE.update(original)

        rate_no_aging = starvation_rate(0.0)
        rate_with_aging = starvation_rate(0.5)
        self.assertLessEqual(rate_with_aging, rate_no_aging,
                              "Aging should not make starvation worse than the no-aging baseline.")

    def test_no_emergency_starvation_ever(self):
        """Emergency tasks must never wait, aging enabled or not -- aging
        must never let a Background job preempt genuine clinical work."""
        wl = generate_starvation_stress_workload(seed=7, horizon=150)
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(wl))
        engine.run(max_time=150)
        rows, _ = engine.report()
        for r in rows:
            if r["queue"] == "EMERGENCY" and r["response"] is not None:
                self.assertEqual(r["response"], 0,
                                  "Emergency response time must always be zero, regardless of aging.")


class TestMetricsCorrectness(unittest.TestCase):
    def test_single_process_metrics(self):
        """Sanity check: a single process with no contention should have
        zero waiting time and turnaround equal to its burst."""
        procs = [Process("P1", "Solo Task", INTERACTIVE, arrival=0, burst=7, base_priority=5)]
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(procs))
        engine.run(max_time=20)
        rows, util = engine.report()
        r = rows[0]
        self.assertEqual(r["waiting"], 0)
        self.assertEqual(r["turnaround"], 7)
        self.assertEqual(r["response"], 0)
        self.assertEqual(util, 100.0 * 7 / engine.time)


if __name__ == "__main__":
    unittest.main(verbosity=2)
