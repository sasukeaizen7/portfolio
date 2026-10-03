"""A throwaway local PostgreSQL on port 5435, for running the projects without Docker.

    python scripts/local_postgres.py start    # creates .pgdata/ on first run (trust auth, localhost only)
    python scripts/local_postgres.py stop

Afterwards: PG_DSN=postgresql://de:de@localhost:5435/de, the same address docker compose exposes.
Uses the PostgreSQL binaries on PATH or in the default install folders.
"""

import glob
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PGDATA = ROOT / ".pgdata"
PORT = "5435"


def pg_bin(tool: str) -> str:
    found = shutil.which(tool)
    if found:
        return found
    for pattern in (r"C:\Program Files\PostgreSQL\*\bin", "/usr/lib/postgresql/*/bin", "/opt/homebrew/opt/postgresql@*/bin"):
        for d in sorted(glob.glob(pattern), reverse=True):
            exe = Path(d) / (tool + (".exe" if os.name == "nt" else ""))
            if exe.exists():
                return str(exe)
    sys.exit(f"{tool} not found: install PostgreSQL or use docker compose")


def start() -> None:
    quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    if not (PGDATA / "PG_VERSION").exists():
        subprocess.run([pg_bin("initdb"), "-D", str(PGDATA), "-U", "de", "--auth=trust", "-E", "UTF8", "--no-locale"],
                       check=True, **quiet)
    if not (PGDATA / "postmaster.pid").exists():
        subprocess.run([pg_bin("pg_ctl"), "-D", str(PGDATA), "-l", str(PGDATA / "server.log"), "-w",
                        "-o", f"-p {PORT} -c listen_addresses=localhost", "start"], check=True, **quiet)
    exists = subprocess.run([pg_bin("psql"), "-h", "localhost", "-p", PORT, "-U", "de", "-d", "postgres", "-tAc",
                             "select 1 from pg_database where datname = 'de'"], capture_output=True, text=True)
    if exists.stdout.strip() != "1":
        subprocess.run([pg_bin("createdb"), "-h", "localhost", "-p", PORT, "-U", "de", "de"], check=True)
    print(f"PostgreSQL running: postgresql://de:de@localhost:{PORT}/de")


def stop() -> None:
    if (PGDATA / "postmaster.pid").exists():
        subprocess.run([pg_bin("pg_ctl"), "-D", str(PGDATA), "-m", "fast", "stop"], check=False)


if __name__ == "__main__":
    {"start": start, "stop": stop}.get(sys.argv[1] if len(sys.argv) > 1 else "", lambda: sys.exit(__doc__))()
