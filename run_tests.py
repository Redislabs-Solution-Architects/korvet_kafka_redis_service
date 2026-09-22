#!/usr/bin/env python3
"""Run every test suite in the bundle.

    python run_tests.py

No server, no network, no packages required.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUITES = [
    ("service logic", HERE / "tests" / "test_service.py"),
    ("client wire protocol", HERE / "tests" / "test_wire.py"),
    ("dashboard, metrics, workers", HERE / "tests" / "test_dashboard.py"),
    ("kafka topics, producer, consumer, workers", HERE / "tests" / "test_kafka_service.py"),
]


def main() -> int:
    results = []
    for label, path in SUITES:
        print(f"\n{'#' * 72}\n# {label}: {path.name}\n{'#' * 72}", flush=True)
        # flush before handing the terminal to the child, or the parent's
        # buffered header lands after the child's output.
        proc = subprocess.run([sys.executable, str(path)], cwd=str(HERE))
        results.append((label, proc.returncode))

    print(f"\n{'=' * 72}")
    failed = [label for label, rc in results if rc != 0]
    for label, rc in results:
        print(f"  {'PASS' if rc == 0 else 'FAIL'}  {label}")
    if failed:
        print(f"\n{len(failed)} suite(s) failed")
        return 1
    print("\nAll suites passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
