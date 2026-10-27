/* ==========================================================================
   Checkpoint-Based Recovery page
   Every number on this page comes from GET /api/recovery/status; every button
   calls a real backend operation (see backend/server.py). Nothing is mocked.
   ========================================================================== */
(function () {
    "use strict";

    const $ = (id) => document.getElementById(id);
    const PX_PER_TICK = 14;
    const POLL_MS = 6000;

    let status = null;      // last status received from the backend
    let busy = false;       // an action is in flight
    let online = null;      // null = unknown, true/false after first contact

    /* ------------------------------------------------------------------ */
    /* helpers                                                              */
    /* ------------------------------------------------------------------ */

    const QCLASS = { EMERGENCY: "q0", INTERACTIVE: "q1", BACKGROUND: "q2" };

    function fmtTime(iso) {
        if (!iso) return "—";
        const d = new Date(iso);
        return isNaN(d) ? "—" : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }

    function fmtDateTime(iso) {
        if (!iso) return "—";
        const d = new Date(iso);
        return isNaN(d) ? "—" : d.toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
    }

    function setText(id, value) { $(id).textContent = value; }

    function setBanner(kind, html) {
        const el = $("stateBanner");
        el.className = "banner " + kind;
        el.innerHTML = html;
    }

    /* ------------------------------------------------------------------ */
    /* talking to the backend                                               */
    /* ------------------------------------------------------------------ */

    function showOffline(message) {
        const el = $("offlineBanner");
        el.hidden = false;
        el.innerHTML =
            "<strong>Backend not reachable.</strong> " + escapeHtml(message || "") +
            "<br>Start it from the project folder with <code>python3 backend/server.py</code> " +
            "and open <code>http://127.0.0.1:8000/pages/recovery.html</code>.";
    }

    function markOnline() {
        $("offlineBanner").hidden = true;
        if (online === false) showToast("Reconnected to the backend.", "success");
        online = true;
        if (window.MedicoreNav) window.MedicoreNav.refreshStatus();
    }

    async function refresh() {
        try {
            status = await MedicoreApi.get("/api/recovery/status");
            markOnline();
            render();
        } catch (err) {
            if (online !== false && window.MedicoreNav) window.MedicoreNav.refreshStatus();
            online = false;
            showOffline(err.message);
            setBanner("warn", "Waiting for the backend…");
            renderControls();
        }
    }

    /** Run a backend action: busy state, error toast, optional success toast, re-render. */
    async function act(button, request, onSuccess) {
        if (busy) return;
        busy = true;
        renderControls();
        try {
            await withBusy(button, async () => {
                const data = await request();
                markOnline();
                status = data && data.status ? data.status : data;
                render();
                if (onSuccess) onSuccess(data);
            });
        } catch (err) {
            if (err.status === 0) {
                online = false;
                showOffline(err.message);
            }
            showToast(err.message, "error", 6500);
            await refresh();
        } finally {
            busy = false;
            renderControls();
        }
    }

    /* ------------------------------------------------------------------ */
    /* rendering                                                            */
    /* ------------------------------------------------------------------ */

    function render() {
        if (!status) return;
        renderPill();
        renderBanner();
        renderTiles();
        renderLanes();
        renderProcesses();
        renderFailSelect();
        renderGantt();
        renderCheckpoints();
        renderLog();
        renderControls();
    }

    const PILL = {
        NOT_STARTED: ["NOT STARTED", ""],
        HEALTHY: ["HEALTHY", "ok"],
        DEGRADED: ["DEGRADED — PROCESSES FAILED", "bad"],
        DOWN: ["SCHEDULER DOWN", "bad"],
        COMPLETED: ["ALL PROCESSES COMPLETED", "done"],
    };

    function renderPill() {
        const [label, cls] = PILL[status.system_status] || [status.system_status, ""];
        const pill = $("systemPill");
        pill.textContent = label;
        pill.className = "status-pill " + cls;
    }

    function renderBanner() {
        const s = status;
        const latest = s.latest_valid_checkpoint;
        const latestTxt = latest
            ? `Latest valid checkpoint: <strong>#${latest.id}</strong> at tick <strong>${latest.tick}</strong>.`
            : "No valid checkpoint is available.";

        switch (s.system_status) {
            case "NOT_STARTED":
                setBanner("info", "No session yet. Choose a workload and press <strong>Start new session</strong>.");
                break;
            case "DOWN":
                if (s.crash_info) {
                    setBanner("error",
                        `<strong>Scheduler is down</strong> (simulated crash at tick ${s.crash_info.tick}). ` +
                        `All in-memory queues were lost. ${latestTxt} Press <strong>Restore latest valid checkpoint</strong>.`);
                } else {
                    setBanner("error",
                        `<strong>No live scheduler.</strong> The backend was restarted, but ${s.checkpoint_count} ` +
                        `checkpoint(s) survived on disk. ${latestTxt} Press <strong>Restore latest valid checkpoint</strong> to resume.`);
                }
                break;
            case "DEGRADED": {
                const failed = s.processes.filter((p) => p.state === "FAILED" || p.state === "UNRECOVERABLE");
                const names = failed.map((p) => `${escapeHtml(p.pid)} (${escapeHtml(p.name)})`).join(", ");
                setBanner("warn",
                    `<strong>${failed.length} process(es) failed:</strong> ${names}. ` +
                    "Press <strong>Recover failed processes</strong>. Healthy processes keep running meanwhile.");
                break;
            }
            case "COMPLETED":
                setBanner("success", `<strong>All ${s.counts.total} processes completed</strong> at tick ${s.time}. ` +
                    `CPU utilisation ${s.utilisation}%.`);
                break;
            default: {
                const lr = s.last_restore;
                const extra = lr
                    ? ` Last restore: checkpoint #${lr.checkpoint_id}, ${lr.work_lost_ticks ?? "unknown"} ticks of simulated work lost.`
                    : "";
                setBanner("success", `<strong>Scheduler healthy</strong> at tick ${s.time}.${extra} ${latestTxt}`);
            }
        }
    }

    function renderTiles() {
        const s = status;
        const live = s.live;
        const c = s.counts;
        const sysLabel = { NOT_STARTED: "—", HEALTHY: "OK", DEGRADED: "DEGRADED", DOWN: "DOWN", COMPLETED: "DONE" }[s.system_status];
        setText("vSystem", sysLabel);
        $("tileSystem").className = "tile " + ({ HEALTHY: "good", COMPLETED: "good", DEGRADED: "alert", DOWN: "alert" }[s.system_status] || "");
        $("vSystem").style.fontSize = "20px";

        setText("vTick", live ? s.time : "—");
        setText("vActive", live ? c.active : "—");
        setText("vFailed", live ? c.failed : "—");
        $("tileFailed").className = "tile" + (live && c.failed > 0 ? " alert" : "");
        setText("vRecovered", live ? c.recovered : "—");
        setText("vRecoveredSub", s.metrics.avg_process_recovery_ms != null ? `avg ${s.metrics.avg_process_recovery_ms} ms` : "");
        setText("vCompleted", live ? `${c.completed}/${c.total}` : "—");

        const latest = s.latest_valid_checkpoint;
        setText("vCheckpoint", latest ? `#${latest.id} · t=${latest.tick}` : "—");
        setText("vCheckpointSub", latest ? fmtDateTime(latest.created_at) : "none saved");

        const lr = s.last_restore;
        const m = s.metrics;
        setText("vRestoreMs", lr ? `${lr.recovery_ms} ms` : (m.avg_restore_ms != null ? `${m.avg_restore_ms} ms` : "—"));
        setText("vRestoreSub", m.scheduler_restores ? `${m.scheduler_restores} restore(s)` + (lr ? "" : " · average") : "no restore yet");

        setText("vLost", lr && lr.work_lost_ticks != null ? `${lr.work_lost_ticks} ticks` : (m.total_work_lost_ticks ? "—" : "0 ticks"));
        setText("vLostSub", `${m.total_work_lost_ticks} ticks total · ${m.failures_injected} failure(s)`);

        setText("vAttempts", m.failed_recovery_attempts);
        setText("vAttemptsSub", `max ${s.constants.max_recovery_attempts} per process`);
    }

    function renderLanes() {
        const s = status;
        const lanes = [
            ["EMERGENCY", "q0", "Q0 EMERGENCY"],
            ["INTERACTIVE", "q1", "Q1 INTERACTIVE"],
            ["BACKGROUND", "q2", "Q2 BACKGROUND"],
        ];
        $("lanes").innerHTML = lanes.map(([name, cls, title]) => {
            const pids = (s.queues && s.queues[name]) || [];
            const chips = pids.map((pid) => {
                const p = s.processes.find((x) => x.pid === pid);
                const boosted = p && p.recovery_boost_ticks > 0;
                return `<span class="chip${boosted ? " boost" : ""}" title="${escapeHtml(p ? p.name : pid)}${boosted ? " — recovery boost active" : ""}">` +
                       `${escapeHtml(pid)}${boosted ? " ⚡" : ""}</span>`;
            }).join("");
            return `<div class="lane ${cls}"><h3><span>${title}</span><span>${pids.length}</span></h3>` +
                   `<div class="chips">${chips || '<span class="cell-sub">empty</span>'}</div></div>`;
        }).join("");
    }

    const STATE_BADGE = {
        NOT_ARRIVED: ["NOT ARRIVED", ""],
        READY: ["READY", "info"],
        RUNNING: ["RUNNING", "ok"],
        COMPLETED: ["COMPLETED", "ok"],
        FAILED: ["FAILED", "bad"],
        UNRECOVERABLE: ["UNRECOVERABLE", "bad"],
    };

    function renderProcesses() {
        const s = status;
        const body = $("procBody");
        if (!s.processes.length) {
            body.innerHTML = `<tr><td colspan="10" class="skeleton">${
                s.system_status === "DOWN" ? "Scheduler is down — restore a checkpoint to see its process table." : "No session."
            }</td></tr>`;
            return;
        }
        body.innerHTML = s.processes.map((p) => {
            const [label, cls] = STATE_BADGE[p.state] || [p.state, ""];
            const pct = p.burst ? Math.round((100 * p.executed) / p.burst) : 0;
            const barCls = p.state === "COMPLETED" ? "done" : (p.state === "FAILED" || p.state === "UNRECOVERABLE" ? "failed" : "");
            const moved = p.current_queue && p.current_queue !== p.queue
                ? `<div class="cell-sub">now in ${escapeHtml(p.current_queue)} (aging)</div>` : "";
            const boost = p.recovery_boost_ticks > 0
                ? `<span class="badge-tag ok" title="Temporary, bounded; reorders work inside ${escapeHtml(p.queue)} only">⚡ ${p.recovery_boost_ticks} ticks left</span>`
                : (p.recovered ? '<span class="cell-sub">recovered · expired</span>' : '<span class="cell-sub">—</span>');
            let action = "";
            if (p.state === "READY" || p.state === "RUNNING") {
                action = `<button class="btn small danger" data-fail="${escapeHtml(p.pid)}" data-needs="live">Fail</button>`;
            } else if (p.state === "FAILED") {
                action = `<button class="btn small success" data-recover="${escapeHtml(p.pid)}" data-needs="live">Recover</button>`;
            }
            return `<tr>
                <td><strong>${escapeHtml(p.pid)}</strong></td>
                <td>${escapeHtml(p.name)}</td>
                <td><span class="queue-tag ${QCLASS[p.queue]}">${escapeHtml(p.queue)}</span>${moved}</td>
                <td><span class="badge-tag ${cls}">${label}</span></td>
                <td style="min-width:130px">
                    <div class="progress ${barCls}"><i style="width:${pct}%"></i></div>
                    <div class="cell-sub">${p.executed}/${p.burst} ticks · ${p.remaining} left</div>
                </td>
                <td>${p.base_priority}</td>
                <td>${p.critical ? '<span class="badge-tag warn">CRITICAL</span>' : '<span class="cell-sub">—</span>'}</td>
                <td>${p.recovery_attempts ? `${p.recovery_attempts}/${s.constants.max_recovery_attempts}` : "0"}</td>
                <td>${boost}</td>
                <td>${action}</td>
            </tr>`;
        }).join("");
    }

    function renderFailSelect() {
        const select = $("fFailPid");
        const previous = select.value;
        const candidates = (status.processes || []).filter((p) => p.state === "READY" || p.state === "RUNNING");
        select.innerHTML = candidates.length
            ? candidates.map((p) => `<option value="${escapeHtml(p.pid)}">${escapeHtml(p.pid)} — ${escapeHtml(p.name)} (${p.executed}/${p.burst})</option>`).join("")
            : '<option value="">No active process</option>';
        if (candidates.some((p) => p.pid === previous)) select.value = previous;
    }

    function renderGantt() {
        const el = $("gantt");
        const s = status;
        if (!s.live) {
            el.innerHTML = `<div class="empty-note">${s.system_status === "DOWN" ? "Scheduler is down — timeline unavailable until restore." : "Start a session to see the schedule."}</div>`;
            return;
        }
        if (!s.gantt.length) {
            el.innerHTML = '<div class="empty-note">No ticks executed yet. Use the Execute controls.</div>';
            return;
        }
        const width = Math.max(s.time, 1) * PX_PER_TICK;
        const rows = s.processes.map((p) => {
            const blocks = s.gantt.filter((g) => g[2] === p.pid).map((g) =>
                `<div class="gantt-block ${QCLASS[g[3]]}" style="left:${g[0] * PX_PER_TICK}px;width:${(g[1] - g[0]) * PX_PER_TICK - 1}px" title="${escapeHtml(p.pid)} t=${g[0]}–${g[1]}"></div>`
            ).join("");
            return `<div class="gantt-row"><div class="pid-label">${escapeHtml(p.pid)}</div>` +
                   `<div class="gantt-track" style="width:${width}px">${blocks}</div></div>`;
        }).join("");

        let ticks = "";
        for (let t = 0; t <= s.time; t += 5) {
            ticks += `<span class="tick-label" style="left:${t * PX_PER_TICK}px">${t}</span>`;
        }
        const marks = (s.checkpoints || []).filter((c) => c.valid && c.tick <= s.time).map((c) =>
            `<span class="ckpt-mark" style="left:${c.tick * PX_PER_TICK}px" title="Checkpoint #${c.id} at t=${c.tick}">▲</span>`
        ).join("");

        el.innerHTML = rows +
            `<div class="gantt-scale" style="margin-left:46px;width:${width}px">${ticks}${marks}</div>`;
        el.scrollLeft = el.scrollWidth;
    }

    function renderCheckpoints() {
        const body = $("cpBody");
        const list = status.checkpoints || [];
        if (!list.length) {
            body.innerHTML = '<tr><td colspan="7" class="skeleton">No checkpoints saved yet.</td></tr>';
            return;
        }
        const latestId = status.latest_valid_checkpoint ? status.latest_valid_checkpoint.id : null;
        body.innerHTML = list.map((c) => {
            const integrity = c.valid
                ? '<span class="badge-tag ok">VALID</span>'
                : `<span class="badge-tag bad" title="${escapeHtml(c.invalid_reason || "")}">INVALID</span><div class="cell-sub">${escapeHtml(c.invalid_reason || "")}</div>`;
            const restore = c.valid
                ? `<button class="btn small" data-restore="${c.id}" data-needs="checkpoints">Restore</button>` : "";
            return `<tr>
                <td><strong>#${c.id}</strong>${c.id === latestId ? ' <span class="badge-tag info">LATEST</span>' : ""}</td>
                <td>${escapeHtml(fmtDateTime(c.created_at))}</td>
                <td>${c.tick}</td>
                <td>${escapeHtml(c.kind)}${c.label && c.label !== c.kind ? `<div class="cell-sub">${escapeHtml(c.label)}</div>` : ""}</td>
                <td>${c.completed} / ${c.active} / ${c.failed}</td>
                <td>${integrity}</td>
                <td>${restore}</td>
            </tr>`;
        }).join("");
    }

    function renderLog() {
        const list = $("logList");
        const events = status.events || [];
        if (!events.length) {
            list.innerHTML = '<div class="skeleton">No events yet.</div>';
            return;
        }
        const LABEL = { success: "OK", error: "ERROR", warn: "WARN", info: "INFO" };
        list.innerHTML = events.map((e) =>
            `<div class="log-item ${escapeHtml(e.level)}">
                <span class="log-time">${escapeHtml(fmtTime(e.ts))}${e.tick != null ? ` · t=${e.tick}` : ""}</span>
                <span class="lvl">${LABEL[e.level] || escapeHtml(e.level)}</span>
                <span>${escapeHtml(e.message)}</span>
            </div>`).join("");
    }

    /** Enable/disable controls according to what is currently possible. */
    function renderControls() {
        const s = status;
        const can = {
            live: !!(s && s.live) && online !== false,
            checkpoints: !!(s && s.latest_valid_checkpoint) && online !== false,
            failed: !!(s && s.live && s.counts.failed > 0) && online !== false,
        };
        document.querySelectorAll("[data-needs]").forEach((el) => {
            if (el.getAttribute("aria-busy") === "true") return;
            el.disabled = busy || !can[el.dataset.needs];
        });
        $("btnStart").disabled = busy || online === false;
        $("btnReset").disabled = busy || online === false;
        if (s && s.live && $("fFailPid").options.length && !$("fFailPid").value) $("btnFail").disabled = true;
    }

    /* ------------------------------------------------------------------ */
    /* actions                                                              */
    /* ------------------------------------------------------------------ */

    function failRecoverySetting() { return parseInt($("fFailRecov").value, 10) || 0; }

    function failProcess(button, pid) {
        if (!pid) { showToast("Choose a process to fail.", "error"); return; }
        act(button, () => MedicoreApi.post("/api/recovery/fail-process", { pid, recovery_failures: failRecoverySetting() }),
            () => showToast(`${pid} crashed (simulated). Run recovery to restore it from a checkpoint.`, "error"));
    }

    function recoverProcesses(button, pid) {
        act(button, () => MedicoreApi.post("/api/recovery/recover", pid ? { pid } : {}), (data) => {
            const recovered = data.results.filter((r) => r.outcome === "recovered");
            const failed = data.results.filter((r) => r.outcome === "attempt_failed");
            const dead = data.results.filter((r) => r.outcome === "unrecoverable");
            if (recovered.length) {
                const lost = recovered.reduce((n, r) => n + r.work_lost_ticks, 0);
                showToast(`Recovered ${recovered.map((r) => r.pid).join(", ")} — ${lost} ticks of simulated work lost.`, "success");
            }
            if (failed.length) showToast(`Recovery attempt failed for ${failed.map((r) => r.pid).join(", ")}. See the log.`, "error", 6000);
            if (dead.length) showToast(`${dead.map((r) => r.pid).join(", ")} exhausted all recovery attempts.`, "error", 6000);
        });
    }

    function restore(button, id) {
        act(button, () => MedicoreApi.post("/api/recovery/restore", id ? { checkpoint_id: id } : {}), () => {
            const lr = status.last_restore;
            showToast(`Restored checkpoint #${lr.checkpoint_id} (tick ${lr.tick}) in ${lr.recovery_ms} ms` +
                      (lr.work_lost_ticks != null ? `; ${lr.work_lost_ticks} ticks of simulated work lost.` : "."), "success", 6000);
            if (lr.skipped_invalid) showToast(`${lr.skipped_invalid} invalid checkpoint(s) were skipped.`, "info", 6000);
        });
    }

    function bind() {
        $("btnStart").addEventListener("click", (e) => act(e.currentTarget, () => MedicoreApi.post("/api/recovery/session", {
            workload: $("fWorkload").value,
            seed: parseInt($("fSeed").value, 10) || 0,
            auto_interval: parseInt($("fAuto").value, 10) || 0,
        }), () => showToast("New session started; initial checkpoint saved.", "success")));

        document.querySelectorAll("[data-step]").forEach((btn) =>
            btn.addEventListener("click", () => act(btn, () => MedicoreApi.post("/api/recovery/step", { ticks: parseInt(btn.dataset.step, 10) }), (d) => {
                if (d.result === "blocked") showToast("Scheduler is waiting: only failed processes remain. Run recovery.", "info", 6000);
                if (d.result === "done") showToast("All processes completed.", "success");
            })));

        $("btnRunAll").addEventListener("click", (e) => act(e.currentTarget, () => MedicoreApi.post("/api/recovery/step", { until_done: true }), (d) => {
            if (d.result === "blocked") showToast("Stopped: only failed processes remain. Run recovery to continue.", "info", 6000);
            else showToast(`Ran ${d.advanced} ticks.`, "success");
        }));

        $("btnCheckpoint").addEventListener("click", (e) => act(e.currentTarget,
            () => MedicoreApi.post("/api/recovery/checkpoint", { label: $("fLabel").value.trim() }),
            (d) => { $("fLabel").value = ""; showToast(`Checkpoint #${d.checkpoint_id} saved at tick ${d.tick}.`, "success"); }));

        $("btnInterrupted").addEventListener("click", (e) => act(e.currentTarget,
            () => MedicoreApi.post("/api/recovery/interrupted-write"),
            (d) => showToast(`Partial checkpoint #${d.checkpoint_id} written. It will be detected and skipped on restore.`, "info", 6000)));

        $("btnFail").addEventListener("click", (e) => failProcess(e.currentTarget, $("fFailPid").value));

        $("btnCrash").addEventListener("click", (e) => act(e.currentTarget, () => MedicoreApi.post("/api/recovery/crash-scheduler"),
            () => showToast("Scheduler crashed (simulated). Restore a checkpoint to resume.", "error", 6000)));

        $("btnRestore").addEventListener("click", (e) => restore(e.currentTarget, null));
        $("btnRecover").addEventListener("click", (e) => recoverProcesses(e.currentTarget, null));

        $("btnReset").addEventListener("click", (e) => {
            if (!confirm("Delete ALL stored checkpoints, sessions and the recovery log? This cannot be undone.")) return;
            act(e.currentTarget, () => MedicoreApi.post("/api/recovery/reset"), () => showToast("Stored recovery data cleared.", "info"));
        });

        // buttons that are re-rendered with the tables
        document.addEventListener("click", (e) => {
            const btn = e.target.closest("button");
            if (!btn || btn.disabled) return;
            if (btn.dataset.fail) failProcess(btn, btn.dataset.fail);
            else if (btn.dataset.recover) recoverProcesses(btn, btn.dataset.recover);
            else if (btn.dataset.restore) restore(btn, parseInt(btn.dataset.restore, 10));
        });

        $("fWorkload").addEventListener("change", () => { $("fSeed").disabled = $("fWorkload").value !== "random"; });
        $("fSeed").disabled = true;
    }

    document.addEventListener("DOMContentLoaded", () => {
        bind();
        renderControls();
        refresh();
        setInterval(() => { if (!busy) refresh(); }, POLL_MS);
    });
})();
