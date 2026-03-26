# Combined Panels Public Copy

Sanitized public copy of:

- `combined_panels`
- `tape_standalone`
- `whale_absorption_standalone`

This copy is prepared for GitHub open-source publishing and CI/CD testing.

## Secret handling

- No runtime `.env` files are included.
- `BEA_TOKEN` is intentionally missing in this public copy.
- BEA integration is still supported and documented via each module's `.env.example`.

To enable BEA later, create:

- `tape_standalone/.env`
- `whale_absorption_standalone/.env`

and set `BEA_TOKEN=<your_token>`.

## Quick Start (No BEA Key Required)

```bash
cd combined_panels_public
python3 -m venv combined_panels/.venv
combined_panels/.venv/bin/python -m pip install -r combined_panels/requirements.txt -r tape_standalone/requirements.txt -r whale_absorption_standalone/requirements.txt
combined_panels/.venv/bin/python combined_panels/run.py
```

Then open:

- `http://127.0.0.1:8090/`
- `http://127.0.0.1:8090/health`

Without a BEA token, the app still runs and retains all exchange-based feeds. BEA-specific endpoints remain available but will report missing token until configured.

## Enable BEA Later

Create local runtime env files (do not commit them):

```bash
cp tape_standalone/.env.example tape_standalone/.env
cp whale_absorption_standalone/.env.example whale_absorption_standalone/.env
```

Then edit both `.env` files and set:

```env
BEA_TOKEN=your_real_token_here
```

Restart the app after updating env vars.

