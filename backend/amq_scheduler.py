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

from dataclasses import dataclass, field
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

    # ---- helpers ---------------------------------------------------------
    def _admit_arrivals(self):
        for p in self.all_processes:
            if p.arrival == self.time and p not in self.queues[p.queue] \
               and p.completion_time is None and p not in self._flat():
                self.queues[p.queue].append(p)

    def _flat(self):
        return self.queues[EMERGENCY] + self.queues[INTERACTIVE] + self.queues[BACKGROUND]

    def _pick_emergency(self) -> Optional[Process]:
        if not self.queues[EMERGENCY]:
            return None
        self.queues[EMERGENCY].sort(key=lambda p: p.dynamic_priority(self.time))
        return self.queues[EMERGENCY][0]

    def _pick_interactive(self) -> Optional[Process]:
        if not self.queues[INTERACTIVE]:
            return None
        return self.queues[INTERACTIVE][0]     # RR: head of queue

    def _pick_background(self) -> Optional[Process]:
        if not self.queues[BACKGROUND]:
            return None
        return self.queues[BACKGROUND][0]      # FCFS: head of queue

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
    def run(self, max_time: int = 200):
        current: Optional[Process] = None
        current_queue: Optional[int] = None

        while self.time < max_time:
            self._admit_arrivals()
            self._apply_aging()

            if all(p.completion_time is not None for p in self.all_processes):
                break

            # --- Emergency preemption check happens every tick ---
            emergency_candidate = self._pick_emergency()

            if emergency_candidate is not None:
                chosen, chosen_q = emergency_candidate, EMERGENCY
                # If we were running something else, put it back at the head
                # of its queue (preempted work is not penalised / not sent to tail).
                if current is not None and current is not chosen:
                    if current not in self.queues[current_queue]:
                        self.queues[current_queue].insert(0, current)
            elif self._pick_interactive() is not None:
                chosen, chosen_q = self._pick_interactive(), INTERACTIVE
            elif self._pick_background() is not None:
                chosen, chosen_q = self._pick_background(), BACKGROUND
            else:
                chosen, chosen_q = None, None

            if chosen is None:
                self.cpu_idle_ticks += 1
                self.time += 1
                continue

            # remove from its queue while it runs this tick
            if chosen in self.queues[chosen_q]:
                self.queues[chosen_q].remove(chosen)

            if chosen.first_run_time is None:
                chosen.first_run_time = self.time

            # execute one tick
            self.gantt.append((self.time, self.time + 1, chosen.pid, QUEUE_NAMES[chosen_q]))
            chosen.remaining -= 1
            self.recent_cpu[chosen.pid] = self.recent_cpu.get(chosen.pid, 0) + 1
            self.time += 1
            self._admit_arrivals()

            if chosen.remaining <= 0:
                chosen.completion_time = self.time
                self.completed.append(chosen)
                current, current_queue = None, None
                continue

            # decide requeue behaviour per policy
            if chosen_q == EMERGENCY:
                chosen.waiting_since = self.time
                self.queues[EMERGENCY].append(chosen)
                current, current_queue = chosen, EMERGENCY
            elif chosen_q == INTERACTIVE:
                chosen.quantum_left -= 1
                if chosen.quantum_left <= 0 or self._pick_emergency() is not None:
                    chosen.quantum_left = TIME_QUANTUM_INTERACTIVE
                    chosen.waiting_since = self.time
                    self.queues[INTERACTIVE].append(chosen)
                    current, current_queue = None, None
                else:
                    self.queues[INTERACTIVE].insert(0, chosen)
                    current, current_queue = chosen, INTERACTIVE
            else:  # BACKGROUND (FCFS => never voluntarily yields except to preemption)
                self.queues[BACKGROUND].insert(0, chosen)
                current, current_queue = chosen, BACKGROUND

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
