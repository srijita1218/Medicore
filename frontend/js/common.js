/* ==========================================================================
   Medicore — shared helpers (loaded on every page)
   Navigation lives in nav.js; this file holds escaping, toasts, busy-button
   handling and the backend API client.
   ========================================================================== */

/** Escape text before putting it into innerHTML. */
function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

/** Small non-blocking notification. type: "success" | "error" | "info". */
function showToast(message, type = "info", durationMs = 4500) {
    let stack = document.getElementById("toastStack");
    if (!stack) {
        stack = document.createElement("div");
        stack.id = "toastStack";
        stack.className = "toast-stack";
        stack.setAttribute("role", "status");
        stack.setAttribute("aria-live", "polite");
        document.body.appendChild(stack);
    }
    const toast = document.createElement("div");
    toast.className = "toast " + type;
    toast.textContent = message;
    stack.appendChild(toast);
    while (stack.children.length > 3) stack.firstChild.remove();   // keep the stack readable
    setTimeout(() => toast.remove(), durationMs);
}

/** Disable a button and show a spinner while an async action runs. */
async function withBusy(button, action) {
    if (button) {
        button.disabled = true;
        button.setAttribute("aria-busy", "true");
    }
    try {
        return await action();
    } finally {
        if (button) {
            button.disabled = false;
            button.removeAttribute("aria-busy");
        }
    }
}

/* --------------------------------------------------------------------------
   Backend API client.
   The backend (python3 backend/server.py) serves both the pages and /api, so
   the same-origin base "" is tried first. If the pages were opened some other
   way (file://, a different static server), fall back to localhost:8000.
   -------------------------------------------------------------------------- */
const MedicoreApi = (() => {
    let base = null;

    class ApiError extends Error {
        constructor(message, status) {
            super(message);
            this.status = status;
        }
    }

    function candidates() {
        const list = [];
        if (location.protocol === "http:" || location.protocol === "https:") {
            list.push("");
        }
        list.push("http://127.0.0.1:8000", "http://localhost:8000");
        return list;
    }

    async function timedFetch(url, options, timeoutMs) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        try {
            return await fetch(url, { ...options, signal: controller.signal });
        } finally {
            clearTimeout(timer);
        }
    }

    async function detect() {
        if (base !== null) return base;
        for (const candidate of candidates()) {
            try {
                const res = await timedFetch(candidate + "/api/health", {}, 1500);
                if (res.ok) {
                    base = candidate;
                    return base;
                }
            } catch (err) { /* try next */ }
        }
        throw new ApiError("Backend is not reachable. Start it with: python3 backend/server.py", 0);
    }

    async function request(method, path, body) {
        const root = await detect();
        let res;
        try {
            res = await timedFetch(root + path, {
                method,
                headers: body !== undefined ? { "Content-Type": "application/json" } : {},
                body: body !== undefined ? JSON.stringify(body) : undefined,
            }, 15000);
        } catch (err) {
            base = null;   // re-detect next time (backend may have restarted)
            throw new ApiError("Lost connection to the backend. Is python3 backend/server.py still running?", 0);
        }
        let data = null;
        try { data = await res.json(); } catch (err) { /* non-JSON error page */ }
        if (!res.ok) {
            throw new ApiError((data && data.error) || `Request failed (${res.status})`, res.status);
        }
        return data;
    }

    async function isOnline() {
        try {
            base = null;
            await detect();
            return true;
        } catch (err) {
            return false;
        }
    }

    return {
        ApiError,
        get: (path) => request("GET", path),
        post: (path, body = {}) => request("POST", path, body),
        isOnline,
    };
})();
