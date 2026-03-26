# Whale Absorption Detector (Module 2)

Standalone real-time module for detecting likely whale absorption events using multi-source BTC flow.

## Features

- Multi-source trade ingestion (BEA + direct WS connectors)
- Unified notional normalization
- Anomaly + out-of-order protection
- Absorption scoring engine (0-100)
- Live API + SSE stream + clean UI
- Source health status and capabilities endpoint

## Run

```bash
cd whale_absorption_standalone
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn server:app --host 127.0.0.1 --port 8878 --reload
```

Open:

- http://127.0.0.1:8878

## Key Endpoints

- `GET /api/absorption/live`
- `GET /api/absorption/stream`
- `GET /api/absorption/stats`
- `GET /api/sources/status`
- `GET /api/capabilities`

## Notes

- `BEA_ONLY` behavior can be tested in the UI using source mode toggle.
- Detection thresholds are controlled by `ABS_*` env variables.
