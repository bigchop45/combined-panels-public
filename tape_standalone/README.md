# Tape (BEA)

Canvas tape fed from **Bleeding Edge Alpha** `/api/v1/market/tape`. The browser never sees your token: a tiny **FastAPI proxy** adds `Authorization: Bearer …`.

## Setup

```bash
cd tape_standalone
cp .env.example .env
# put BEA_TOKEN in .env
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn server:app --host 127.0.0.1 --port 8765
```

Or run without activating: `tape_standalone/.venv/bin/uvicorn server:app --host 127.0.0.1 --port 8765` from the `tape_standalone` directory.

Open **http://127.0.0.1:8765**

Change symbol in the bar (e.g. `BTC-USD`) and blur/change the field to refetch.

## API

- **GET** `/api/bea/tape?symbol=BTC-USD` — JSON passthrough from BEA (shape may vary; `public/tape.js` normalizes common wrappers).

## Embed elsewhere

1. Copy **`public/tape.js`** (or import as a module).
2. Serve a **same-origin** proxy like `server.py` (do not expose `BEA_TOKEN` in frontend env).
3. `renderer.startBeaFeed({ endpoint: '/api/bea/tape', symbol: 'BTC-USD', pollMs: 1200 })`

For local demo only you can use simulation: `renderer.startSimulation()` (no BEA).

## Field mapping

`tape.js` maps BEA rows with flexible keys: `price` / `px`, `usd` / `size_usd` / `size*price`, `side`, `t` / `ts` (ms or s), `exchange` / `venue`, etc. If your payload uses different names, extend `rowToTapePrint` in `tape.js`.
