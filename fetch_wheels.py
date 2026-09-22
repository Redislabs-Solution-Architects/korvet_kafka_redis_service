#!/usr/bin/env python3
"""Download Streamlit's wheels for offline install. Run on a machine WITH internet.

The rest of this bundle needs no packages at all. This script exists only for the
optional Streamlit dashboard; `dashboard.py` gives the same charts with nothing
installed, so treat this as a convenience rather than a requirement.

On an internet-connected machine, with the SAME Python version, OS and CPU
architecture as the target host:

    python fetch_wheels.py

Then copy the whole bundle across and, on the airgapped host:

    pip install --no-index --find-links wheels streamlit

Platform matters: wheels for manylinux x86_64 will not install on macOS arm64.
Pass --python-version / --platform to fetch for a different target than the
machine you are running on, or just run this on a matching box.

The PoC hosts from the setup summary are Red Hat 9 x86_64, so from a Linux
machine the defaults are already correct.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

PACKAGES = ["streamlit", "pandas"]  # pandas gives nicer time-axis charts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download Streamlit wheels for an offline install.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--dest", default="wheels", help="output directory")
    p.add_argument("--packages", nargs="+", default=PACKAGES)
    p.add_argument("--python-version",
                   help="target Python, e.g. 3.11 (default: this interpreter)")
    p.add_argument("--platform", action="append", dest="platforms",
                   help="target platform tag, e.g. manylinux2014_x86_64. "
                        "Repeatable. Implies --only-binary :all:")
    p.add_argument("--index-url", help="internal PyPI mirror, if you have one")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "pip", "download", *args.packages, "-d", str(dest)]
    if args.index_url:
        cmd += ["--index-url", args.index_url]
    if args.python_version:
        cmd += ["--python-version", args.python_version, "--only-binary", ":all:"]
    for plat in (args.platforms or []):
        cmd += ["--platform", plat]
    if args.platforms and "--only-binary" not in cmd:
        # pip refuses --platform without it, and the error message is obscure.
        cmd += ["--only-binary", ":all:"]

    print("running:", " ".join(cmd), "\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("\npip download failed. Common causes:\n"
              "  - no internet on this machine (that is what this script needs)\n"
              "  - a package has no wheel for the requested --platform\n"
              "  - your organisation requires --index-url pointing at a mirror\n",
              file=sys.stderr)
        return result.returncode

    files = sorted(dest.iterdir())
    total = sum(f.stat().st_size for f in files if f.is_file())
    print(f"\nDownloaded {len(files)} files, {total / 1_048_576:.1f} MB into {dest}/")
    print(f"\nThis interpreter: Python {sys.version_info.major}."
          f"{sys.version_info.minor} on {sys.platform}")
    print("The target host must match, or the wheels will not install.\n")
    print("On the airgapped host:")
    print(f"    pip install --no-index --find-links {dest} {' '.join(args.packages)}")
    print("    streamlit run streamlit_app.py\n")
    print("No internet and no pip at all? Use the zero-dependency dashboard:")
    print("    python dashboard.py\n")

    if shutil.which("pip") is None:
        print("note: 'pip' was not found on PATH here; used "
              f"{sys.executable} -m pip instead")
    return 0


if __name__ == "__main__":
    sys.exit(main())
