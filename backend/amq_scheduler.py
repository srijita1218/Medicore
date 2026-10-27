"""
Adaptive Multilevel Queue (AMQ) CPU Scheduler Simulator
For Hospital Management Systems

Authors : Kopal Joshi (24BCI0116), Srijita Kumar (24BCI0113)
Purpose : Review-2 prototype demonstrating the proposed scheduler:
            Queue 0 - EMERGENCY   : Preemptive Priority Scheduling
            Queue 1 - INTERACTIVE : Round Robin (quantum = 4 ticks)
            Queue 2 - BACKGROUND  : FCFS
          plus a dynamic-priority / aging mechanism that raises the
          effective priority of processes that have waited too long,
          preventing starvation without weakening emergency preemption.
"""

from dataclasses import dataclass, field, fields
from typing import List, Optional
import copy

# --------------------------------------------------------------------------
# 1. Process model
# --------------------------------------------------------------------------

EMERGENCY, INTERACTIVE, BACKGROUND = 0, 1, 2
QUEUE_NAMES = {EMERGENCY: "EMERGENCY", INTERACTIVE: "INTERACTIVE", BACKGROUND: "BACKGROUND"}

# Aging slope per queue: how fast effective priority improves per tick waited.
# Used both for (a) the dynamic-priority score below, and (b) the queue-
# promotion threshold in AdaptiveMultilevelScheduler._apply_aging().
AGING_SLOPE = {EMERGENCY: 0.0, INTERACTIVE: 0.5, BACKGROUND: 0.2}
TIME_QUANTUM_INTERACTIVE = 4

# A BACKGROUND process that has waited AGING_PROMOTION_BASE / AGING_SLOPE[BACKGROUND]
# ticks without running is promoted into the INTERACTIVE queue (classic
# multilevel-FEEDBACK-queue aging). Slope 0 disables promotion entirely,
# which is used deliberately in experiments.py to show the starvation
# failure mode of a non-aging multilevel queue.
# e.g. slope=0.2 (proposed default) -> promote after 40 ticks waiting.
#      slope=1.0 (aggressive)       -> promote after 8 ticks waiting.
AGING_PROMOTION_BASE = 8

# --- Checkpoint-based recovery (see checkpoint_recovery.py) -----------------
# A recovered *critical* process receives a TEMPORARY, BOUNDED scheduling boost
# *inside its own queue only*. The boost never changes ``Process.queue`` (the
# normal queue assignment), never lets a process jump into a higher queue, and
# therefore can never preempt a genuine EMERGENCY process. It is measured in
# CPU ticks actually consumed, so it expires on its own.
RECOVERY_BOOST_TICKS = 6
MAX_RECOVERY_ATTEMPTS = 3


@dataclass
class Process:
    pid: str
    name: str
    queue: int              # EMERGENCY / INTERACTIVE / BACKGROUND
    arrival: int             # arrival time (ticks)
    burst: int               # total CPU burst needed (ticks)
    base_priority: int       # lower value = higher priority (used within EMERGENCY)
    remaining: int = field(init=False)
    waiting_since: int = field(init=False)   # last tick the process became ready/re-queued
    wait_time: int = 0
    first_run_time: Optional[int] = None
    completion_time: Optional[int] = None
    quantum_left: int = TIME_QUANTUM_INTERACTIVE
    promotions: int = 0        # how many times aging promoted this process to a higher queue
    # --- recovery metadata (all default to "never failed", so existing code is unaffected) ---
    critical: bool = False             # critical service: eligible for the bounded recovery boost
    failed: bool = False               # crashed and waiting for recovery
    recovered: bool = False            # has been restored from a checkpoint at least once
    recovery_attempts: int = 0         # recovery attempts made (successful or not)
    recovery_failures_pending: int = 0 # simulated: next N recovery attempts will fail
    boost_ticks_left: int = 0          # temporary recovery priority (CPU ticks); 0 = none
    failed_at: Optional[int] = None    # scheduler tick at which the failure happened
    remaining_at_failure: Optional[int] = None

    def __post_init__(self):
        self.remaining = self.burst
        self.waiting_since = self.arrival

    def dynamic_priority(self, now: int, recent_cpu_usage: int = 0) -> float:
        """
        Priority = f(base_priority, waiting_time, emergency_level, recent_cpu_usage)
        Lower score => scheduled sooner.
        Emergency queue: base_priority dominates (clinical severity), aging is 0
        so a *more* severe alert always wins, but ties are broken by arrival.
        Interactive/Background: aging slowly erodes base_priority so long-waiting
        tasks eventually win over freshly-arrived, lower-urgency ones (anti-starvation).
        """
        waited = now - self.waiting_since
        aging_bonus = AGING_SLOPE[self.queue] * waited
        cpu_penalty = 0.1 * recent_cpu_usage  # discourage CPU-hungry procs a little
        return self.base_priority - aging_bonus + cpu_penalty

    # ---- (de)serialisation, used by checkpointing -----------------------
    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, data: dict) -> "Process":
        init_names = {f.name for f in fields(cls) if f.init}
        proc = cls(**{k: v for k, v in data.items() if k in init_names})
        for f in fields(cls):
            if not f.init and f.name in data:
                setattr(proc, f.name, data[f.name])
        for key, value in data.items():
            if key in init_names:
                setattr(proc, key, value)
        return proc


# --------------------------------------------------------------------------
# 2. Scheduler
# --------------------------------------------------------------------------

class AdaptiveMultilevelScheduler:
    def __init__(self, processes: List[Process]):
        # master list sorted by arrival; queues are populated as procs "arrive"
        self.all_processes = sorted(processes, key=lambda p: p.arrival)
        self.queues = {EMERGENCY: [], INTERACTIVE: [], BACKGROUND: []}
        self.time = 0
        self.gantt = []           # list of (start, end, pid, queue) execution slices
        self.completed = []
        self.cpu_idle_ticks = 0
        self.recent_cpu = {}      # pid -> ticks used in last window (simple counter)
        self.current: Optional[Process] = None        # process that ran last tick (kept for preemption bookkeeping)
        self.current_queue: Optional[int] = None

    # ---- helpers ---------------------------------------------------------
    def _admit_arrivals(self):
        for p in self.all_processes:
            if p.arrival == self.time and p not in self.queues[p.queue] \
               and p.completion_time is None and not p.failed and p not in self._flat():
                self.queues[p.queue].append(p)

    def _flat(self):
        return self.queues[EMERGENCY] + self.queues[INTERACTIVE] + self.queues[BACKGROUND]

    def _pick_emergency(self) -> Optional[Process]:
        if not self.queues[EMERGENCY]:
            return None
        self.queues[EMERGENCY].sort(key=lambda p: p.dynamic_priority(self.time))
        return self.queues[EMERGENCY][0]

    @staticmethod
    def _boosted_or_head(queue: List[Process]) -> Optional[Process]:
        """Head of the queue, unless a recovered critical process still holds a
        temporary recovery boost -- then the first such process goes first.
        With no boosted process this is exactly the original head-of-queue pick."""
        if not queue:
            return None
        for p in queue:
            if p.boost_ticks_left > 0:
                return p
        return queue[0]

    def _pick_interactive(self) -> Optional[Process]:
        return self._boosted_or_head(self.queues[INTERACTIVE])   # RR: head of queue

    def _pick_background(self) -> Optional[Process]:
        return self._boosted_or_head(self.queues[BACKGROUND])    # FCFS: head of queue

    def _requeue_tail(self, q, proc):
        q.remove(proc)
        proc.waiting_since = self.time
        q.append(proc)

    def _apply_aging(self):
        """
        Promote any BACKGROUND process that has waited past its aging
        threshold into the INTERACTIVE queue, so it starts competing for
        CPU via Round Robin instead of waiting indefinitely for the
        machine to go fully idle. This is the mechanism that actually
        prevents starvation (the dynamic_priority() score alone only
        affects *ordering within* the EMERGENCY queue).

        Background is intentionally never promoted into EMERGENCY: a
        purely administrative task must never preempt genuine clinical
        alerts, regardless of how long it has waited.
        """
        slope = AGING_SLOPE[BACKGROUND]
        if slope <= 0:
            return  # aging disabled for this configuration (used to demonstrate starvation)
        threshold = AGING_PROMOTION_BASE / slope

        for p in list(self.queues[BACKGROUND]):
            if (self.time - p.waiting_since) >= threshold:
                self.queues[BACKGROUND].remove(p)
                p.waiting_since = self.time
                p.quantum_left = TIME_QUANTUM_INTERACTIVE
                p.promotions += 1
                self.queues[INTERACTIVE].append(p)

    # ---- main loop ---------------------------------------------------------
    def step(self) -> str:
        """Advance the simulation by exactly one scheduling decision (one tick).

        Returns "ran" (a process executed), "idle" (CPU idle), "done" (every
        process has completed) or "blocked" (only crashed processes remain, so
        the scheduler is waiting for recovery and time does not advance).
        ``run()`` is just this method in a loop, so stepping and running share
        one implementation of the AMQ policies.
        """
        self._admit_arrivals()
        self._apply_aging()

        if all(p.completion_time is not None for p in self.all_processes):
            return "done"
        if all(p.completion_time is not None or p.failed for p in self.all_processes):
            return "blocked"      # degraded: nothing runnable until failed work is recovered

        # --- Emergency preemption check happens every tick ---
        emergency_candidate = self._pick_emergency()

        if emergency_candidate is not None:
            chosen, chosen_q = emergency_candidate, EMERGENCY
            # If we were running something else, put it back at the head
            # of its queue (preempted work is not penalised / not sent to tail).
            if self.current is not None and self.current is not chosen:
                if self.current not in self.queues[self.current_queue]:
                    self.queues[self.current_queue].insert(0, self.current)
        elif self._pick_interactive() is not None:
            chosen, chosen_q = self._pick_interactive(), INTERACTIVE
        elif self._pick_background() is not None:
            chosen, chosen_q = self._pick_background(), BACKGROUND
        else:
            chosen, chosen_q = None, None

        if chosen is None:
            self.cpu_idle_ticks += 1
            self.time += 1
            return "idle"

        # remove from its queue while it runs this tick
        if chosen in self.queues[chosen_q]:
            self.queues[chosen_q].remove(chosen)

        if chosen.first_run_time is None:
            chosen.first_run_time = self.time

        # execute one tick
        self.gantt.append((self.time, self.time + 1, chosen.pid, QUEUE_NAMES[chosen_q]))
        chosen.remaining -= 1
        if chosen.boost_ticks_left > 0:
            chosen.boost_ticks_left -= 1          # recovery boost is spent per CPU tick used
        self.recent_cpu[chosen.pid] = self.recent_cpu.get(chosen.pid, 0) + 1
        self.time += 1
        self._admit_arrivals()

        if chosen.remaining <= 0:
            chosen.completion_time = self.time
            chosen.boost_ticks_left = 0
            self.completed.append(chosen)
            self.current, self.current_queue = None, None
            return "ran"

        # decide requeue behaviour per policy
        if chosen_q == EMERGENCY:
            chosen.waiting_since = self.time
            self.queues[EMERGENCY].append(chosen)
            self.current, self.current_queue = chosen, EMERGENCY
        elif chosen_q == INTERACTIVE:
            chosen.quantum_left -= 1
            if chosen.quantum_left <= 0 or self._pick_emergency() is not None:
                chosen.quantum_left = TIME_QUANTUM_INTERACTIVE
                chosen.waiting_since = self.time
                self.queues[INTERACTIVE].append(chosen)
                self.current, self.current_queue = None, None
            else:
                self.queues[INTERACTIVE].insert(0, chosen)
                self.current, self.current_queue = chosen, INTERACTIVE
        else:  # BACKGROUND (FCFS => never voluntarily yields except to preemption)
            self.queues[BACKGROUND].insert(0, chosen)
            self.current, self.current_queue = chosen, BACKGROUND
        return "ran"

    def run(self, max_time: int = 200):
        while self.time < max_time:
            if self.step() in ("done", "blocked"):
                break
        self._merge_gantt()

    def _merge_gantt(self):
        """Collapse consecutive identical-pid slices into single bars for a cleaner chart."""
        merged = []
        for slot in self.gantt:
            if merged and merged[-1][2] == slot[2] and merged[-1][1] == slot[0]:
                s, e, pid, q = merged[-1]
                merged[-1] = (s, slot[1], pid, q)
            else:
                merged.append(slot)
        self.gantt = merged

    # ---- state snapshot / restore (checkpointing) ----------------------------
    def to_state(self) -> dict:
        """Complete, JSON-serialisable snapshot of the scheduler: process table,
        queue contents *in order*, clock, Gantt history and the bookkeeping the
        policies depend on. Restoring it and stepping on is indistinguishable
        from never having stopped."""
        return {
            "time": self.time,
            "idle": self.cpu_idle_ticks,
            "current": self.current.pid if self.current is not None else None,
            "current_queue": self.current_queue,
            "queues": {str(q): [p.pid for p in self.queues[q]] for q in (EMERGENCY, INTERACTIVE, BACKGROUND)},
            "completed": [p.pid for p in self.completed],
            "recent_cpu": dict(self.recent_cpu),
            "gantt": [list(slot) for slot in self.gantt],
            "processes": [p.to_dict() for p in self.all_processes],
        }

    @classmethod
    def from_state(cls, state: dict) -> "AdaptiveMultilevelScheduler":
        """Rebuild a scheduler from ``to_state()`` output. Raises ValueError if the
        snapshot is internally inconsistent (callers treat that as a corrupt checkpoint)."""
        try:
            procs = [Process.from_dict(d) for d in state["processes"]]
            by_pid = {p.pid: p for p in procs}
            if len(by_pid) != len(procs):
                raise ValueError("duplicate pid in process table")
            for p in procs:
                if p.queue not in QUEUE_NAMES:
                    raise ValueError(f"{p.pid}: invalid queue {p.queue!r}")
                if not (0 <= p.remaining <= p.burst):
                    raise ValueError(f"{p.pid}: remaining {p.remaining} outside 0..{p.burst}")
            time_now = state["time"]
            if not isinstance(time_now, int) or time_now < 0:
                raise ValueError("invalid clock value")

            sched = cls(procs)
            sched.all_processes = procs            # keep the saved order exactly
            sched.time = time_now
            sched.cpu_idle_ticks = int(state["idle"])
            seen = set()
            for q_key, pids in state["queues"].items():
                q = int(q_key)
                if q not in QUEUE_NAMES:
                    raise ValueError(f"invalid queue key {q_key!r}")
                for pid in pids:
                    if pid not in by_pid:
                        raise ValueError(f"queue references unknown pid {pid!r}")
                    if pid in seen:
                        raise ValueError(f"{pid} appears in more than one queue slot")
                    seen.add(pid)
                    proc = by_pid[pid]
                    if proc.completion_time is not None or proc.failed:
                        raise ValueError(f"{pid} is queued but completed/failed")
                    sched.queues[q].append(proc)
            sched.completed = [by_pid[pid] for pid in state["completed"]]
            if any(p.completion_time is None for p in sched.completed):
                raise ValueError("completed list contains an unfinished process")
            sched.recent_cpu = {str(k): int(v) for k, v in state["recent_cpu"].items()}
            sched.gantt = [tuple(slot) for slot in state["gantt"]]
            cur = state["current"]
            if cur is not None:
                if cur not in by_pid:
                    raise ValueError(f"current references unknown pid {cur!r}")
                sched.current = by_pid[cur]
                sched.current_queue = int(state["current_queue"])
            return sched
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValueError(f"malformed scheduler state: {exc!r}") from exc

    # ---- failure / recovery hooks ----------------------------------------------
    def get_process(self, pid: str) -> Process:
        for p in self.all_processes:
            if p.pid == pid:
                return p
        raise KeyError(f"unknown process {pid!r}")

    def queue_of(self, proc: Process) -> Optional[int]:
        """Queue *list* the process currently sits in (may differ from proc.queue
        after aging promotion), or None if it is not queued."""
        for q in (EMERGENCY, INTERACTIVE, BACKGROUND):
            if any(x is proc for x in self.queues[q]):
                return q
        return None

    def _detach(self, proc: Process):
        for q in self.queues:
            self.queues[q][:] = [x for x in self.queues[q] if x is not proc]
        if self.current is proc:
            self.current, self.current_queue = None, None

    def fail_process(self, pid: str, recovery_failures: int = 0) -> Process:
        """Simulate a crash: the process leaves every queue and is marked FAILED.
        Its progress is *not* rolled back here -- that is the recovery manager's
        job, driven by a checkpoint."""
        proc = self.get_process(pid)
        if proc.completion_time is not None:
            raise ValueError(f"{pid} has already completed; nothing to fail")
        if proc.failed:
            raise ValueError(f"{pid} has already failed")
        if self.queue_of(proc) is None:
            raise ValueError(f"{pid} has not arrived yet; only active processes can fail")
        self._detach(proc)
        proc.failed = True
        proc.failed_at = self.time
        proc.remaining_at_failure = proc.remaining
        proc.boost_ticks_left = 0
        proc.recovery_failures_pending = max(0, int(recovery_failures))
        return proc

    def reinstate(self, proc: Process, remaining: int, queue_level: int, boost_ticks: int = 0):
        """Put a recovered process back into a queue with checkpointed progress.
        ``proc.queue`` (its normal assignment) is untouched; ``boost_ticks`` is the
        separate, temporary recovery priority."""
        if queue_level not in QUEUE_NAMES:
            raise ValueError(f"invalid queue level {queue_level!r}")
        proc.remaining = remaining
        proc.failed = False
        proc.recovered = True
        proc.waiting_since = self.time
        proc.quantum_left = TIME_QUANTUM_INTERACTIVE
        proc.boost_ticks_left = boost_ticks
        self.queues[queue_level].append(proc)

    # ---- metrics ---------------------------------------------------------
    def report(self):
        rows = []
        for p in sorted(self.all_processes, key=lambda x: x.pid):
            executed = p.burst - p.remaining
            if p.completion_time is not None:
                turnaround = p.completion_time - p.arrival
                waiting = turnaround - p.burst
                response = (p.first_run_time - p.arrival) if p.first_run_time is not None else None
                starved = False
            else:
                # Never finished within the simulation horizon -> genuine starvation.
                turnaround = None
                waiting = (self.time - p.arrival) - executed
                response = (p.first_run_time - p.arrival) if p.first_run_time is not None else None
                starved = True
            rows.append({
                "pid": p.pid, "name": p.name, "queue": QUEUE_NAMES[p.queue],
                "arrival": p.arrival, "burst": p.burst,
                "completion": p.completion_time, "turnaround": turnaround,
                "waiting": waiting, "response": response, "promotions": p.promotions,
                "starved": starved,
            })
        total_time = self.time
        utilisation = 100 * (total_time - self.cpu_idle_ticks) / total_time
        return rows, utilisation
