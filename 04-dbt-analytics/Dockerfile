FROM python:3.12-slim
WORKDIR /dbt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN dbt deps --profiles-dir .
ENTRYPOINT ["dbt"]
CMD ["build", "--profiles-dir", "."]
