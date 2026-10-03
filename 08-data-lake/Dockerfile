FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
COPY lake/ lake/
COPY sql/ sql/
COPY tests/ tests/
