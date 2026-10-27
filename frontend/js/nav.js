/* ==========================================================================
   Medicore — shared site header + navigation
   One component for every page. Include it once per page:

       <div id="medicore-nav"></div>
       <script src="../js/nav.js" defer></script>      (path relative to the page)

   Link targets are resolved against this script's own location, so they are
   correct from the root page, from /pages/*, and when the site is served from
   a sub-folder. The active page is detected from the URL, so it works no
   matter which page the user opens first.
   ========================================================================== */
(function () {
    "use strict";

    const scriptEl =
        document.currentScript ||
        document.querySelector('script[src$="js/nav.js"], script[src$="nav.js"]');
    // .../frontend/js/nav.js  ->  .../frontend/
    const ROOT = new URL("../", scriptEl ? scriptEl.src : window.location.href);

    const LINKS = [
        { label: "Dashboard",           path: "index.html" },
        { label: "Scheduler",           path: "pages/scheduler.html" },
        { label: "Processes",           path: "pages/processes.html" },
        { label: "Queues",              path: "pages/queues.html" },
        { label: "Metrics",             path: "pages/metrics.html" },
        { label: "Experiments",         path: "pages/experiments.html" },
        { label: "Checkpoint Recovery", path: "pages/recovery.html", isNew: true },
    ];

    function normalise(pathname) {
        let p = decodeURIComponent(pathname);
        if (p.endsWith("/")) p += "index.html";
        return p;
    }

    function render() {
        const mount = document.getElementById("medicore-nav");
        if (!mount) return;

        const here = normalise(window.location.pathname);
        const items = LINKS.map((link) => {
            const url = new URL(link.path, ROOT);
            const active = normalise(url.pathname) === here;
            return `<a href="${url.href}"${active ? ' class="active" aria-current="page"' : ""}` +
                   `${link.isNew && !active ? ' class="nav-new"' : ""}>${link.label}</a>`;
        }).join("");

        mount.innerHTML =
            `<header class="topbar site-header">
                <div>
                    <div class="brand">MEDICORE</div>
                    <div class="subtitle">Adaptive Multilevel Queue CPU Scheduler</div>
                </div>
                <div class="top-status" id="siteStatus" title="Connection to the Medicore backend">● CHECKING BACKEND…</div>
            </header>
            <nav class="main-nav" aria-label="Main navigation">${items}</nav>`;

        updateBackendStatus();
    }

    async function updateBackendStatus() {
        const el = document.getElementById("siteStatus");
        if (!el) return;
        const online = typeof MedicoreApi !== "undefined" ? await MedicoreApi.isOnline() : false;
        el.textContent = online ? "● BACKEND ONLINE" : "● BACKEND OFFLINE";
        el.className = "top-status " + (online ? "ok" : "bad");
        el.title = online
            ? "Connected to the Medicore backend."
            : "Backend not reachable. Scheduler simulation still works; Checkpoint Recovery needs: python3 backend/server.py";
    }

    window.MedicoreNav = { refreshStatus: updateBackendStatus };

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", render);
    } else {
        render();
    }
})();
