"""
Randomised / trace-driven workload generator.

Generates reproducible (seeded) synthetic hospital workloads so the
scheduler can be evaluated over many runs instead of a single fixed
example, as flagged in the Review-2 "next steps".
"""

import random
from amq_scheduler import Process, EMERGENCY, INTERACTIVE, BACKGROUND

# Realistic-ish ranges per queue, loosely modelled on the kinds of
# tasks named in the proposal (ECG/ICU alerts, dashboards/EMR, billing/backup).
PROFILES = {
    EMERGENCY:   {"names": ["ECG Alert", "ICU Monitor Spike", "Code Blue", "SpO2 Critical Alarm",
                            "Arrhythmia Alert", "BP Critical Alarm"],
                  "arrival_gap": (0, 8), "burst": (1, 4), "priority": (0, 2)},
    INTERACTIVE: {"names": ["Doctor Dashboard", "EMR Lookup", "Nurse Chart Update",
                            "Lab Result View", "Prescription Entry", "Discharge Summary"],
                  "arrival_gap": (0, 5), "burst": (4, 12), "priority": (4, 6)},
    BACKGROUND:  {"names": ["Billing Batch", "Nightly Backup", "Report Generation",
                            "Log Rotation", "Data Warehouse Sync", "Archive Cleanup"],
                  "arrival_gap": (0, 10), "burst": (8, 20), "priority": (8, 10)},
}


def generate_workload(seed: int, n_emergency: int = 3, n_interactive: int = 4,
                       n_background: int = 3):
    """
    Build a reproducible random workload. Same seed -> identical workload,
    which is what lets experiments.py compare scheduler variants fairly.
    """
    rng = random.Random(seed)
    processes = []
    counts = {EMERGENCY: n_emergency, INTERACTIVE: n_interactive, BACKGROUND: n_background}
    pid_counter = 1

    for queue, count in counts.items():
        profile = PROFILES[queue]
        t = 0
        for i in range(count):
            t += rng.randint(*profile["arrival_gap"])
            burst = rng.randint(*profile["burst"])
            priority = rng.randint(*profile["priority"])
            name = rng.choice(profile["names"]) + f" #{i+1}"
            processes.append(Process(f"P{pid_counter}", name, queue, arrival=t,
                                      burst=burst, base_priority=priority))
            pid_counter += 1

    return processes


def generate_starvation_stress_workload(seed: int, horizon: int = 150):
    """
    Adversarial workload: Interactive tasks arrive steadily throughout the
    simulation horizon (keeping that queue busy roughly 70-85% of the
    time), a handful of Emergency alerts are sprinkled in, and two
    moderately long Background jobs arrive at t=0. Under a *strict*
    "background runs only when the machine is fully idle" policy, the
    small idle slivers left over are not enough for the Background jobs
    to complete inside the horizon -- this is the concrete starvation
    scenario the promotion-based aging in amq_scheduler.py is designed
    to fix by letting a long-waiting Background job join the Interactive
    Round-Robin pool instead of waiting for total idleness.
    """
    rng = random.Random(seed)
    processes = []
    pid = 1

    # Background: two administrative jobs, both ready from the start.
    for i in range(2):
        burst = rng.randint(14, 20)
        processes.append(Process(f"P{pid}", f"Background Job #{i+1}", BACKGROUND,
                                  arrival=0, burst=burst, base_priority=9))
        pid += 1

    # Emergency: a few short, sparse alerts across the horizon.
    t = rng.randint(5, 15)
    while t < horizon:
        burst = rng.randint(1, 3)
        processes.append(Process(f"P{pid}", "Emergency Alert", EMERGENCY,
                                  arrival=t, burst=burst, base_priority=rng.randint(0, 2)))
        pid += 1
        t += rng.randint(25, 40)

    # Interactive: steady stream at roughly 65-70% load, so the queue is busy
    # most, but not all, of the time -- enough idle slivers exist that a
    # strict "background only when fully idle" policy is *close* to
    # working, which is exactly where aging vs. no-aging diverges.
    t = 0
    while t < horizon:
        burst = rng.randint(2, 4)
        processes.append(Process(f"P{pid}", "Interactive Task", INTERACTIVE,
                                  arrival=t, burst=burst, base_priority=5))
        pid += 1
        t += rng.randint(3, 6)

    return processes


if __name__ == "__main__":
    for seed in (1, 2, 3):
        wl = generate_workload(seed)
        print(f"\n--- Workload (seed={seed}) ---")
        for p in wl:
            print(f"  {p.pid:4s} {p.name:28s} queue={p.queue} arrival={p.arrival:3d} "
                  f"burst={p.burst:3d} base_priority={p.base_priority}")
