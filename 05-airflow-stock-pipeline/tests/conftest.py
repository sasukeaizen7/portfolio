import sys
from pathlib import Path

# Airflow puts include/ on the path inside the containers; do the same for the tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
