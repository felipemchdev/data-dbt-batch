# RUNBOOK - Lakehouse Olist

## 1) Pré-requisitos

- Docker e Docker Compose
- Ou Python 3.11+ com `pip`
- CSVs Olist no diretório `data/raw/`

## 2) Ordem oficial do pipeline

1. ingest raw (`src.ingest.check_raw_files`)
2. bronze load (`src.ingest.load_bronze`)
3. quality check (`src.ge.validate_bronze`)
4. dbt silver (`dbt run --select silver`)
5. dbt gold (`dbt run --select gold`)
6. dbt tests (`dbt test`)
7. analytics (`src.analytics.generate_report`)

## 3) Execução manual rápida

```bash
python -m src.ingest.check_raw_files
python -m src.ingest.load_bronze
python -m src.ge.validate_bronze
cd dbt
dbt run --select silver --profiles-dir .
dbt run --select gold --profiles-dir .
dbt test --profiles-dir .
cd ..
python -m src.analytics.generate_report
```

## 4) Execução via Airflow

```bash
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Dispare a DAG `lakehouse_olist_pipeline` na UI do Airflow.

## 5) Troubleshooting

- `CSV não encontrado`: valide nomes de arquivo em `data/raw/`.
- `Quality gate falhou`: corrija nulos/colunas/valores inválidos na fonte.
- `dbt source not found`: execute bronze load antes do dbt.
- `DuckDB lock`: feche processos concorrentes apontando para `data/lakehouse.duckdb`.

## 6) Evidências esperadas de sucesso

- `data/raw/raw_manifest.json`
- `data/bronze/*.parquet`
- schemas `bronze`, `silver`, `gold` no DuckDB
- `data/gold/report.html`
- `data/gold/exports/*.csv`

## 7) Operação recorrente

- Scheduler do Airflow: diário (`@daily`).
- Reprocessamento: rerun da DAG (pipeline idempotente).
- Monitoramento básico: logs no Airflow e stdout dos scripts Python.
