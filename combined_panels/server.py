"""
Combined module workspace.

Loads standalone FastAPI modules, mounts them under one server, and exposes
module metadata for the front-end dashboard layout system.
"""
from __future__ import annotations

import importlib.util
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

COMBINED_BASE = Path(__file__).resolve().parent
PROJECT_ROOT = COMBINED_BASE.parent
PUBLIC = COMBINED_BASE / "public"


@dataclass(frozen=True)
class ModuleSpec:
    module_id: str
    title: str
    mount_path: str
    server_path: Path
    startup_name: str = "_startup"
    shutdown_name: str = "_shutdown"
    default_x: int = 24
    default_y: int = 24
    default_w: int = 980
    default_h: int = 700
    enabled: bool = True


MODULE_SPECS: list[ModuleSpec] = [
    ModuleSpec(
        module_id="tape",
        title="Tape",
        mount_path="/tape",
        server_path=PROJECT_ROOT / "tape_standalone" / "server.py",
        default_x=24,
        default_y=24,
        default_w=1180,
        default_h=760,
    ),
    ModuleSpec(
        module_id="absorption",
        title="Whale Absorption",
        mount_path="/absorption",
        server_path=PROJECT_ROOT / "whale_absorption_standalone" / "server.py",
        default_x=1240,
        default_y=24,
        default_w=920,
        default_h=760,
    ),
]


def _load_fastapi_app(module_path: Path):
    name = f"panel_{module_path.stem}_{abs(hash(str(module_path))) % 1_000_000_000}"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    sub = getattr(mod, "app", None)
    if sub is None:
        raise RuntimeError(f"No `app` in {module_path}")
    return sub, mod


loaded_modules: list[dict[str, Any]] = []
module_load_errors: list[dict[str, str]] = []

for spec in MODULE_SPECS:
    if not spec.enabled:
        continue
    try:
        if not spec.server_path.exists():
            raise FileNotFoundError(f"Missing module file: {spec.server_path}")
        sub_app, mod = _load_fastapi_app(spec.server_path)
        loaded_modules.append(
            {
                "spec": spec,
                "app": sub_app,
                "mod": mod,
            }
        )
    except Exception as e:
        module_load_errors.append(
            {
                "module_id": spec.module_id,
                "mount_path": spec.mount_path,
                "error": str(e),
            }
        )


@asynccontextmanager
async def _combined_lifespan(_: FastAPI):
    started: list[dict[str, Any]] = []
    for entry in loaded_modules:
        spec: ModuleSpec = entry["spec"]
        mod = entry["mod"]
        startup = getattr(mod, spec.startup_name, None)
        if callable(startup):
            await startup()
            started.append(entry)
    yield
    for entry in reversed(started):
        spec: ModuleSpec = entry["spec"]
        mod = entry["mod"]
        shutdown = getattr(mod, spec.shutdown_name, None)
        if callable(shutdown):
            await shutdown()


app = FastAPI(
    title="Combined Module Workspace",
    description="Dynamic module workspace with mounted standalone apps.",
    lifespan=_combined_lifespan,
)


@app.get("/health", include_in_schema=False)
async def health() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "service": "combined_panels",
            "loaded_modules": len(loaded_modules),
            "module_load_errors": module_load_errors,
        }
    )


@app.get("/", include_in_schema=False)
async def combined_index() -> FileResponse:
    return FileResponse(PUBLIC / "index.html")


@app.head("/", include_in_schema=False)
async def combined_index_head() -> Response:
    return Response(status_code=200, media_type="text/html; charset=utf-8")


@app.get("/api/modules", include_in_schema=False)
async def modules_meta() -> JSONResponse:
    modules = []
    for entry in loaded_modules:
        spec: ModuleSpec = entry["spec"]
        modules.append(
            {
                "module_id": spec.module_id,
                "title": spec.title,
                "mount_path": spec.mount_path,
                "url": f"{spec.mount_path.lstrip('/')}/",
                "absolute_url": f"{spec.mount_path}/",
                "default_layout": {
                    "x": spec.default_x,
                    "y": spec.default_y,
                    "w": spec.default_w,
                    "h": spec.default_h,
                },
            }
        )
    return JSONResponse({"modules": modules, "errors": module_load_errors})


def _make_redirect_endpoint(url: str):
    async def _redirect() -> RedirectResponse:
        return RedirectResponse(url=url, status_code=307)

    return _redirect


for entry in loaded_modules:
    spec: ModuleSpec = entry["spec"]
    app.add_api_route(
        spec.mount_path,
        _make_redirect_endpoint(f"{spec.mount_path}/"),
        methods=["GET"],
        include_in_schema=False,
        name=f"{spec.module_id}_trailing_slash",
    )
    app.mount(spec.mount_path, entry["app"])
