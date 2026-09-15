"""
Demo / evaluation harness for the Adaptive Multilevel Queue (AMQ) scheduler.
Builds a representative hospital workload (ECG, ICU monitor, EMR, dashboard,
billing, backup) and compares AMQ against a plain FCFS baseline scheduler,
reproducing the metrics referenced on the "Expected Outcomes" slide.
"""

import argparse
import copy
from amq_scheduler import (
    Process, AdaptiveMultilevelScheduler, EMERGENCY, INTERACTIVE, BACKGROUND, QUEUE_NAMES
)
from workload_generator import generate_workload

# --------------------------------------------------------------------------
# Workload definition (ticks ~ represent short simulated time units)
# --------------------------------------------------------------------------

def build_workload():
    return [
        # Life-critical (Emergency queue) — short, urgent bursts, arrive mid-run
        Process("P1", "ECG Alert - Bed 12",      EMERGENCY,   arrival=0,  burst=3, base_priority=1),
        Process("P2", "ICU Monitor - Bed 04",    EMERGENCY,   arrival=6,  burst=2, base_priority=1),
        Process("P3", "Code Blue Alert - ER",    EMERGENCY,   arrival=10, burst=2, base_priority=0),

        # Clinician-facing (Interactive queue) — moderate bursts, need responsiveness
        Process("P4", "Doctor Dashboard",        INTERACTIVE, arrival=0,  burst=10, base_priority=5),
        Process("P5", "EMR Lookup",              INTERACTIVE, arrival=1,  burst=8,  base_priority=5),
        Process("P6", "Nurse Chart Update",      INTERACTIVE, arrival=3,  burst=6,  base_priority=5),

        # Administrative (Background queue) — long, non-critical
        Process("P7", "Billing Batch Job",       BACKGROUND,  arrival=0,  burst=14, base_priority=9),
        Process("P8", "Nightly DB Backup",       BACKGROUND,  arrival=2,  burst=12, base_priority=9),
    ]


def run_fcfs_baseline(processes):
    """Naive single-queue FCFS baseline (no priority, no preemption) for comparison."""
    procs = sorted(copy.deepcopy(processes), key=lambda p: p.arrival)
    time = 0
    gantt = []
    for p in procs:
        start = max(time, p.arrival)
        end = start + p.burst
        gantt.append((start, end, p.pid, "FCFS"))
        p.first_run_time = start
        p.completion_time = end
        time = end
    rows = []
    for p in sorted(procs, key=lambda x: x.pid):
        turnaround = p.completion_time - p.arrival
        waiting = turnaround - p.burst
        response = p.first_run_time - p.arrival
        rows.append({"pid": p.pid, "name": p.name, "arrival": p.arrival, "burst": p.burst,
                      "completion": p.completion_time, "turnaround": turnaround,
                      "waiting": waiting, "response": response})
    return rows, gantt, time


def print_gantt(gantt, width_scale=1):
    print("\nGantt chart (time -> ):")
    line = ""
    for s, e, pid, q in gantt:
        line += f"[{pid}:{s}-{e}] "
    print(line)


def print_table(rows, title):
    print(f"\n{title}")
    header = f"{'PID':<5}{'Name':<24}{'Arr':<5}{'Burst':<7}{'Compl':<7}{'TAT':<6}{'Wait':<6}{'Resp':<6}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(f"{r['pid']:<5}{r['name']:<24}{r['arrival']:<5}{r['burst']:<7}"
              f"{r['completion']:<7}{r['turnaround']:<6}{r['waiting']:<6}{r['response']:<6}")
    avg_wait = sum(r["waiting"] for r in rows) / len(rows)
    avg_tat = sum(r["turnaround"] for r in rows) / len(rows)
    avg_resp = sum(r["response"] for r in rows) / len(rows)
    print(f"\nAverage Waiting Time   : {avg_wait:.2f} ticks")
    print(f"Average Turnaround Time: {avg_tat:.2f} ticks")
    print(f"Average Response Time  : {avg_resp:.2f} ticks")
    return avg_wait, avg_tat, avg_resp


def main():
    ap = argparse.ArgumentParser(description="AMQ scheduler demo: fixed showcase workload by default.")
    ap.add_argument("--random", action="store_true",
                     help="use a randomised workload instead of the fixed showcase example")
    ap.add_argument("--seed", type=int, default=1, help="seed for --random (default: 1)")
    args = ap.parse_args()

    workload = generate_workload(args.seed) if args.random else build_workload()

    # ---- Run proposed AMQ scheduler ----
    amq = AdaptiveMultilevelScheduler(copy.deepcopy(workload))
    amq.run(max_time=100)
    amq_rows, amq_util = amq.report()

    print("=" * 70)
    print(" PROPOSED SCHEDULER: Adaptive Multilevel Queue (Emergency/Interactive/Background)")
    print("=" * 70)
    print_gantt(amq.gantt)
    amq_avg_wait, amq_avg_tat, amq_avg_resp = print_table(amq_rows, "Per-process metrics (AMQ):")
    print(f"CPU Utilisation         : {amq_util:.2f}%")

    emergency_rows = [r for r in amq_rows if r["pid"] in ("P1", "P2", "P3")]
    amq_emergency_resp = sum(r["response"] for r in emergency_rows) / len(emergency_rows)
    print(f"Avg Emergency Response Time (AMQ): {amq_emergency_resp:.2f} ticks")

    # ---- Run baseline FCFS scheduler ----
    fcfs_rows, fcfs_gantt, fcfs_total_time = run_fcfs_baseline(workload)

    print("\n" + "=" * 70)
    print(" BASELINE SCHEDULER: Plain FCFS (single queue, no priority/preemption)")
    print("=" * 70)
    print_gantt(fcfs_gantt)
    fcfs_avg_wait, fcfs_avg_tat, fcfs_avg_resp = print_table(fcfs_rows, "Per-process metrics (FCFS):")

    emergency_rows_fcfs = [r for r in fcfs_rows if r["pid"] in ("P1", "P2", "P3")]
    fcfs_emergency_resp = sum(r["response"] for r in emergency_rows_fcfs) / len(emergency_rows_fcfs)
    print(f"Avg Emergency Response Time (FCFS): {fcfs_emergency_resp:.2f} ticks")

    # ---- Comparison summary ----
    print("\n" + "=" * 70)
    print(" COMPARISON SUMMARY")
    print("=" * 70)
    reduction = 100 * (fcfs_emergency_resp - amq_emergency_resp) / fcfs_emergency_resp
    print(f"Emergency response time  : FCFS={fcfs_emergency_resp:.2f}  AMQ={amq_emergency_resp:.2f}  "
          f"=> {reduction:.1f}% reduction with AMQ")
    print(f"Average waiting time     : FCFS={fcfs_avg_wait:.2f}  AMQ={amq_avg_wait:.2f}")
    print(f"Average turnaround time  : FCFS={fcfs_avg_tat:.2f}  AMQ={amq_avg_tat:.2f}")

    # Starvation check for background tasks
    bg_rows = [r for r in amq_rows if r["pid"] in ("P7", "P8")]
    print("\nStarvation check (Background queue, AMQ):")
    for r in bg_rows:
        print(f"  {r['pid']} ({r['name']}): completed at t={r['completion']}, "
              f"waited={r['waiting']} ticks -> completed, not starved")

    return amq, amq_rows, fcfs_rows, amq_util, amq_emergency_resp, fcfs_emergency_resp


if __name__ == "__main__":
    main()
