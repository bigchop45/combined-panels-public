# Combined module workspace

Single FastAPI process that mounts standalone apps and serves a draggable, resizable workspace.

| Path | App |
|------|-----|
| `/` | Scrollable workspace (dynamic module windows) |
| `/tape/` | Tape UI + `/tape/api/…` |
| `/absorption/` | Absorption UI + `/absorption/api/…` |
| `/api/modules` | Loaded module metadata for frontend rendering |

## Setup

Use the same `.env` patterns as each app (e.g. `BEA_TOKEN` in `tape_standalone/.env` and `whale_absorption_standalone/.env`). Each sub-app loads its own `.env` from its directory.

```bash
cd combined_panels
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn server:app --host 127.0.0.1 --port 8090
```

Run **`uvicorn` from inside `combined_panels`** so `server.py` resolves and `tape_standalone/` / `whale_absorption_standalone/` sit next to this folder. Alternatively, from the repo root: `uvicorn combined_panels.server:app`.

Open **http://127.0.0.1:8090/**

Or: `python3 run.py` from this folder (works even if your shell is elsewhere).

### Nothing loads / blank page

1. **Confirm the process is up:** open **http://127.0.0.1:8090/health** — you should see `{"ok":true,...}`. If the connection fails, the server is not running or another app is bound to the port.
2. **Run from `combined_panels`:** `uvicorn server:app --host 127.0.0.1 --port 8090` must be started **after** `cd` into `combined_panels`, **or** use `python3 run.py`, **or** from the repo root: `uvicorn combined_panels.server:app --host 127.0.0.1 --port 8090`.
3. **Install deps:** `pip install -r requirements.txt` (needs `certifi` etc. so the absorption sub-app can import).
4. **Sibling folders:** `tape_standalone/` and `whale_absorption_standalone/` must sit next to `combined_panels/` (same parent directory as in this repo).

## Is this a separate app?

**Yes.** `combined_panels/` is its own small project (`server.py`, `run.py`, `public/index.html`). It does **not** copy standalone code; it **loads** module apps from sibling folders, mounts them, and exposes `/api/modules` so the workspace can render windows dynamically.

**Feeds in combined mode:** A mounted FastAPI app often **does not** run its own `@app.on_event("startup")`, so websocket/BEA loops would never start unless the combined app’s **lifespan** calls each module’s `_startup()`. `combined_panels/server.py` does that. Check **`feeds_started`: true** and **`background_tasks` > 0** on **http://127.0.0.1:8090/tape/api/tape/debug** after restart.

## “Raw 0 / events 0” — feeds vs logic

| Symptom | Meaning |
|--------|---------|
| **`STREAM` in the bar** | The browser SSE connection to this server is usually OK. |
| **`raw 0` on tape** | The tape **hub** snapshot for that symbol has **no trades** (or UI filter hides everything). |
| **`events 0` on absorption** | Either **no trades** in the whale hub for that symbol, or **no cluster** passed the absorption **scoring** rules (normal to see 0 events often). |

**Diagnose feeds (no guessing):** after starting the combined server, open:

- **http://127.0.0.1:8090/tape/api/tape/debug** — `global_deque_count` / `snapshot_count_for_symbol` and per-source `connected` / `msg_count`.
- **http://127.0.0.1:8090/absorption/api/absorption/debug** — same idea for the whale hub.

If **`global_deque_count` is 0** but **`feeds_started` is false**, the combined server did not run startup (wrong process or old code). If **`feeds_started` is true** but deque is still 0, check **network** / **`.env`**. Before this lifespan fix, **`feeds_started` was never set** in combined mode even though the HTTP server ran.

If counts are **non-zero** but the tape still shows **shown 0**, lower the **min print** threshold (e.g. $50K vs $400K); **shown** applies after size filters.

## Notes

- **Duplicate connections:** both backends connect to exchanges / BEA. That is the same tradeoff as running two terminals with each app, but in one process.
- **Standalone still works:** run `tape_standalone` or `whale_absorption_standalone` alone on their usual ports; frontends use document-relative API URLs so they work both standalone and under `/tape/` or `/absorption/`.

## Expansion pattern (add future modules)

`combined_panels/server.py` has a `MODULE_SPECS` registry. Add one entry per module:

- `module_id`: unique id (`sweep_exhaustion`)
- `title`: window title (`Sweep + Exhaustion`)
- `mount_path`: subpath (`/sweep`)
- `server_path`: sibling `server.py` path
- `startup_name` / `shutdown_name`: defaults `_startup` / `_shutdown`
- `default_x/y/w/h`: initial layout for new users

When added:

1. module is imported and mounted automatically
2. lifespan auto-calls startup/shutdown if those functions exist
3. frontend workspace auto-renders the module from `/api/modules`
4. layout persists in browser localStorage (`combined_panels.layout.v1`)

If a module fails to load, combined server still starts and reports errors in:

- `GET /health`
- `GET /api/modules`
