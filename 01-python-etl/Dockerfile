# Small, reproducible image: pinned Python, dependencies in their own layer (cached between code
# changes), code copied last, and a non-root user.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ etl/
RUN useradd --create-home etl && mkdir -p /app/data && chown etl /app/data
USER etl

ENTRYPOINT ["python", "-m", "etl.cli"]
CMD ["--postgres"]
