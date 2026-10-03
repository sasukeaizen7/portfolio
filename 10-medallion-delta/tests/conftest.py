import sys
from pathlib import Path

# In the container /app/stream is project 09's generator (mounted); locally it's ../09-kafka-streaming.
for candidate in (Path("/app"), Path(__file__).resolve().parents[2] / "09-kafka-streaming"):
    if (candidate / "stream").exists():
        sys.path.insert(0, str(candidate))
        break
