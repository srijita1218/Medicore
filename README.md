# Medicore — Adaptive Multilevel Queue CPU Scheduler

A hospital-themed simulator of an Adaptive Multilevel Queue (AMQ) CPU scheduler.

| Queue | Policy |
|---|---|
| 0 · Emergency | Preemptive priority |
| 1 · Interactive | Round Robin (quantum = 4 ticks) |
| 2 · Background | FCFS + aging-based promotion (starvation prevention) |

Features: preemptive emergency handling, aging/promotion, metrics, FCFS comparison, aging experiments,
and **Checkpoint-Based Recovery** (new, see below).

## Project layout

```
backend/
  amq_scheduler.py        AMQ scheduler (step()/run(), snapshot/restore, failure hooks)
  checkpoint_recovery.py  SQLite checkpoint store + recovery manager (drives the real scheduler)
  server.py               stdlib HTTP server: JSON API (/api/*) + serves ../frontend
  demo.py, experiments.py, workload_generator.py   (unchanged CLI tools)
  test_scheduler.py       original scheduler tests
  test_recovery.py        checkpoint / restore / restart / priority / API tests
frontend/
  index.html, pages/*.html
  js/nav.js               the ONE shared header + navigation component
  js/common.js            helpers (escaping, toasts) and the backend API client
  js/recovery.js          Checkpoint Recovery page
  css/style.css           shared styles
```

## Setup and run

Requirements: **Python 3.9+** only. No packages to install (standard library only).

```bash
git clone https://github.com/srijita1218/Medicore.git && cd Medicore
python3 backend/server.py            # http://127.0.0.1:8000
```

Open <http://127.0.0.1:8000/>. The same server serves the pages and the API, so everything works from one
command. Options: `--port 8080`, `--host 0.0.0.0`, `--db /path/to/file.db` (or env `MEDICORE_DB`).
Checkpoints are stored in `backend/data/medicore.db` (git-ignored).

The Dashboard, Scheduler, Processes, Queues, Metrics and Experiments pages also work without the backend.
Only **Checkpoint Recovery** needs it; the header shows `BACKEND ONLINE / OFFLINE`.

### Tests

```bash
cd backend
python3 -m unittest discover -v     # all tests (scheduler + recovery)
python3 -m unittest test_recovery   # recovery tests only
```

### Existing CLI tools (unchanged)

```bash
python3 backend/demo.py [--random --seed 3]
python3 backend/experiments.py --seeds 30 --out results.csv
```

## Checkpoint-Based Recovery

> **This is a simulation.** "Process failures", "scheduler restarts" and "checkpoints" model the AMQ scheduler
> only. Nothing saves or restores a real operating-system process, kernel state or hardware.

How it works:

- **Same scheduler.** The recovery page drives `AdaptiveMultilevelScheduler` through the backend; there is
  no second scheduler. `run()` was refactored into a `step()` loop with identical results (verified against
  the previous demo/experiment output).
- **Checkpoints** (SQLite, one atomic transaction each) hold the full process table, queue contents in order,
  states, executed/remaining bursts, priorities, aging/quantum counters, Gantt history and the clock, plus a
  SHA-256 checksum. They survive backend restarts.
- **Restore** picks the newest *valid* checkpoint. Corrupt, partially written, wrong-schema or
  logically inconsistent checkpoints are detected, logged and skipped. Incomplete processes keep their progress.
  Resuming from any checkpoint reproduces the uninterrupted schedule exactly (tested at every tick).
- **Process recovery.** A failed process leaves the queues; recovery restores its progress from the newest
  checkpoint in which it was still healthy. Attempts are counted (max 3, then *unrecoverable*), and attempts can be
  made to fail on purpose.
- **Priority without starvation.** Recovery order: genuine emergency processes → other critical services →
  the rest. A recovered critical process gets a *temporary boost measured in CPU ticks* (6) that only reorders work
  **inside its own queue**. `Process.queue` (the normal assignment) never changes, a boosted process can never
  overtake an Emergency process or jump to a higher queue, and the boost expires by itself.
- **Dashboard.** System status, checkpoint timestamps, active/failed/recovered/completed counts, recovery log,
  recovery time (ms) and simulated work lost (ticks).

### Demo script (≈3 minutes)

1. `python3 backend/server.py`, open **Checkpoint Recovery**.
2. **Start new session** (Hospital showcase) → **+10 ticks** → **Create checkpoint**.
3. **+5 ticks**, then pick a process that has already run (e.g. `P4`) → **Fail process**. The system turns
   *DEGRADED*; the other processes keep running.
4. **Recover failed processes**: the process resumes from the checkpointed progress (not from zero), shows a
   temporary ⚡ recovery priority, and the log reports the simulated work lost.
5. **Simulate interrupted write**, then **Crash / restart scheduler** → status *DOWN*.
6. **Restore latest valid checkpoint**: the partial checkpoint is flagged INVALID and skipped, the schedule resumes.
7. **Backend restart:** stop the server (Ctrl+C) and start it again. The page reconnects, shows *DOWN* with the
   checkpoints still on disk; **Restore** and **Run to completion** finish the run.
8. To show failed recovery attempts: set *Make upcoming recovery attempts fail* to "All 3", fail a process, and press
   **Recover** three times → *UNRECOVERABLE*.

### API (for reference)

`GET /api/health`, `GET /api/recovery/status`, `POST /api/recovery/{session,step,checkpoint,interrupted-write,
fail-process,crash-scheduler,restore,recover,reset}` — JSON in/out, errors as `{"error": "..."}`.

## Notes

- `frontend/amq_console.html` is an older standalone console; it is untouched and not linked from the navigation.
- The scheduler page's in-browser simulation (`js/scheduler.js`) is still client-side, as before.
