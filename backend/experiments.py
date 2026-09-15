"""
Experiment harness: runs the AMQ scheduler across many random seeds and
several aging-slope configurations, then reports mean +/- std-dev for
each metric. This is what turns the single-example demo into a
statistically grounded evaluation (the item flagged as a "next step"
in the Review-2 write-up).

Usage:
    python3 experiments.py --seeds 30 --out results.csv
"""

import argparse
import copy
import csv
import statistics as st

import amq_scheduler as sched
from amq_scheduler import AdaptiveMultilevelScheduler, INTERACTIVE, BACKGROUND
from workload_generator import generate_starvation_stress_workload

HORIZON = 150


def run_once(seed: int, aging_interactive: float, aging_background: float, max_time: int = HORIZON):
    # Temporarily patch the module-level aging slopes for this run.
    original = dict(sched.AGING_SLOPE)
    sched.AGING_SLOPE[INTERACTIVE] = aging_interactive
    sched.AGING_SLOPE[BACKGROUND] = aging_background
    try:
        workload = generate_starvation_stress_workload(seed, horizon=max_time)
        engine = AdaptiveMultilevelScheduler(copy.deepcopy(workload))
        engine.run(max_time=max_time)
        rows, util = engine.report()
    finally:
        sched.AGING_SLOPE.update(original)

    finished_rows = [r for r in rows if r["turnaround"] is not None]
    emergency_rows = [r for r in rows if r["queue"] == "EMERGENCY" and r["response"] is not None]
    bg_rows = [r for r in rows if r["queue"] == "BACKGROUND"]
    total_promotions = sum(r["promotions"] for r in bg_rows)

    avg_wait = st.mean(r["waiting"] for r in finished_rows) if finished_rows else 0
    avg_tat = st.mean(r["turnaround"] for r in finished_rows) if finished_rows else 0
    avg_emergency_resp = st.mean(r["response"] for r in emergency_rows) if emergency_rows else 0
    max_bg_wait = max((r["waiting"] for r in bg_rows), default=0)
    starved = any(r["starved"] for r in bg_rows)  # background never finished inside the horizon

    return {
        "seed": seed,
        "aging_interactive": aging_interactive,
        "aging_background": aging_background,
        "avg_wait": avg_wait,
        "avg_turnaround": avg_tat,
        "avg_emergency_response": avg_emergency_resp,
        "max_background_wait": max_bg_wait,
        "cpu_utilisation": util,
        "background_promotions": total_promotions,
        "background_starved": starved,
    }


def summarise(rows, key):
    vals = [r[key] for r in rows]
    return st.mean(vals), (st.stdev(vals) if len(vals) > 1 else 0.0)


def main():
    ap = argparse.ArgumentParser(description="Sweep aging-slope configurations over random seeds.")
    ap.add_argument("--seeds", type=int, default=20, help="number of random seeds per configuration")
    ap.add_argument("--out", type=str, default="experiment_results.csv")
    args = ap.parse_args()

    configs = [
        ("No aging (0, 0)", 0.0, 0.0),
        ("Proposed (0.5, 0.2)", 0.5, 0.2),
        ("Aggressive aging (1.5, 1.0)", 1.5, 1.0),
    ]

    all_rows = []
    print(f"Running {args.seeds} seeds x {len(configs)} aging configurations "
          f"({args.seeds * len(configs)} simulations)...\n")

    for label, ai, ab in configs:
        config_rows = [run_once(seed, ai, ab) for seed in range(1, args.seeds + 1)]
        all_rows.extend(config_rows)

        wait_m, wait_s = summarise(config_rows, "avg_wait")
        tat_m, tat_s = summarise(config_rows, "avg_turnaround")
        er_m, er_s = summarise(config_rows, "avg_emergency_response")
        mbw_m, mbw_s = summarise(config_rows, "max_background_wait")
        util_m, util_s = summarise(config_rows, "cpu_utilisation")
        starved_count = sum(1 for r in config_rows if r["background_starved"])

        print(f"[{label}]")
        print(f"  Avg waiting time        : {wait_m:6.2f} +/- {wait_s:5.2f}")
        print(f"  Avg turnaround time     : {tat_m:6.2f} +/- {tat_s:5.2f}")
        print(f"  Avg emergency response  : {er_m:6.2f} +/- {er_s:5.2f}")
        print(f"  Max background wait     : {mbw_m:6.2f} +/- {mbw_s:5.2f}")
        print(f"  CPU utilisation (%)     : {util_m:6.2f} +/- {util_s:5.2f}")
        print(f"  Background starved runs : {starved_count} / {len(config_rows)}")
        print()

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Raw per-run results written to {args.out}")


if __name__ == "__main__":
    main()
