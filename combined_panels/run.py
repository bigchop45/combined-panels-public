#!/usr/bin/env python3
"""Start combined panels. Safe to run from any directory: `python3 path/to/combined_panels/run.py`"""
from __future__ import annotations

import os
import sys


def main() -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root)
    if root not in sys.path:
        sys.path.insert(0, root)
    import uvicorn

    uvicorn.run("server:app", host="127.0.0.1", port=8090, reload=False)


if __name__ == "__main__":
    main()
