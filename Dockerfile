FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-duckdb \
    && pip install --no-cache-dir -r /tmp/requirements.txt
