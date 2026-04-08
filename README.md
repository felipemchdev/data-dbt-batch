# Lakehouse Olist Batch Pipeline

This project is an end-to-end batch pipeline using Olist data with Bronze/Silver/Gold layers.
It combines Python scripts, dbt models, DuckDB, and Airflow in a practical local setup.

## Stack

- Python
- DuckDB
- dbt (dbt-duckdb)
- Great Expectations
- Apache Airflow
- Docker Compose

## Project Structure

```text
airflow/
  dags/
dbt/
  models/staging/
  models/marts/
src/
  ingest/
  ge/
  analytics/
data/
  raw/
  bronze/
  gold/
```

## Expected Files in `data/raw`

- `customers.csv` or `olist_customers_dataset.csv`
- `orders.csv` or `olist_orders_dataset.csv`
- `order_items.csv` or `olist_order_items_dataset.csv`
- `order_payments.csv` or `olist_order_payments_dataset.csv`
- `products.csv` or `olist_products_dataset.csv`
- `sellers.csv` or `olist_sellers_dataset.csv`
- `geolocation.csv` or `olist_geolocation_dataset.csv`

## Run From Scratch (Docker + Airflow)

1. Start Docker Desktop.
2. Build images:

```bash
docker-compose build dbt airflow-webserver airflow-scheduler
```

3. Initialize Airflow metadata DB:

```bash
docker-compose run --rm airflow-webserver airflow db init
```

4. Create Airflow admin user:

```bash
docker-compose run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname "<your_first_name>" \
  --lastname "<your_last_name>" \
  --role Admin \
  --email "<your_email>"
```

5. Start services:

```bash
docker-compose up -d airflow-webserver airflow-scheduler
```

6. Open Airflow UI:

- URL: `http://localhost:8080`
- User: `admin`
- Password: `admin`

7. Unpause and trigger the main DAG:

```bash
docker-compose exec airflow-scheduler airflow dags unpause lakehouse_olist_pipeline
docker-compose exec airflow-scheduler airflow dags trigger lakehouse_olist_pipeline
```

## Optional: Run dbt Only

```bash
docker-compose run --rm dbt dbt debug
docker-compose run --rm dbt dbt run
docker-compose run --rm dbt dbt test
```

## Outputs

- DuckDB database: `data/lakehouse.duckdb`
- Bronze parquet files: `data/bronze/*.parquet`
- HTML report: `data/gold/report.html`
- KPI summary: `data/gold/kpi_summary.json`
- CSV exports: `data/gold/exports/*.csv`

## See Results in DuckDB UI

Open the correct file:

```bash
duckdb -ui data/lakehouse.duckdb
```

In SQL editor:

```sql
SHOW ALL TABLES;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
ORDER BY 1,2;
```

## Useful Commands

Enter Airflow container:

```bash
docker-compose exec airflow-scheduler bash
```

Stop everything:

```bash
docker-compose down
```

Clean unused Docker resources:

```bash
docker system prune -f
```

## Quick Troubleshooting

- DAGs are not showing in UI
  - Make sure scheduler is running:
  - `docker-compose up -d airflow-scheduler`

- `ModuleNotFoundError: duckdb` in Airflow tasks
  - Rebuild and restart Airflow services:
  - `docker-compose build airflow-webserver airflow-scheduler`
  - `docker-compose up -d --force-recreate airflow-webserver airflow-scheduler`

- `Permission denied` writing under `/app/data`
  - Recreate services with current compose config:
  - `docker-compose up -d --force-recreate airflow-webserver airflow-scheduler`

---
/
---

# Lakehouse Olist Batch Pipeline (PT-BR)

Este projeto é um pipeline batch ponta a ponta com dados da Olist, usando camadas Bronze/Silver/Gold.
Ele junta scripts Python, models dbt, DuckDB e Airflow em um fluxo local simples de subir e testar.

## Stack

- Python
- DuckDB
- dbt (dbt-duckdb)
- Great Expectations
- Apache Airflow
- Docker Compose

## Estrutura do Projeto

```text
airflow/
  dags/
dbt/
  models/staging/
  models/marts/
src/
  ingest/
  ge/
  analytics/
data/
  raw/
  bronze/
  gold/
```

## Arquivos Esperados em `data/raw`

- `customers.csv` ou `olist_customers_dataset.csv`
- `orders.csv` ou `olist_orders_dataset.csv`
- `order_items.csv` ou `olist_order_items_dataset.csv`
- `order_payments.csv` ou `olist_order_payments_dataset.csv`
- `products.csv` ou `olist_products_dataset.csv`
- `sellers.csv` ou `olist_sellers_dataset.csv`
- `geolocation.csv` ou `olist_geolocation_dataset.csv`

## Rodar do Zero (Docker + Airflow)

1. Abra o Docker Desktop.
2. Faça o build das imagens:

```bash
docker-compose build dbt airflow-webserver airflow-scheduler
```

3. Inicialize o banco de metadata do Airflow:

```bash
docker-compose run --rm airflow-webserver airflow db init
```

4. Crie o usuário admin do Airflow:

```bash
docker-compose run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname "<seu_nome>" \
  --lastname "<seu_sobrenome>" \
  --role Admin \
  --email "<seu_email>"
```

5. Suba os serviços:

```bash
docker-compose up -d airflow-webserver airflow-scheduler
```

6. Abra a UI do Airflow:

- URL: `http://localhost:8080`
- Usuário: `admin`
- Senha: `admin`

7. Despause e dispare a DAG principal:

```bash
docker-compose exec airflow-scheduler airflow dags unpause lakehouse_olist_pipeline
docker-compose exec airflow-scheduler airflow dags trigger lakehouse_olist_pipeline
```

## Opcional: Rodar só dbt

```bash
docker-compose run --rm dbt dbt debug
docker-compose run --rm dbt dbt run
docker-compose run --rm dbt dbt test
```

## Saídas do Projeto

- Banco DuckDB: `data/lakehouse.duckdb`
- Arquivos parquet da bronze: `data/bronze/*.parquet`
- Relatório HTML: `data/gold/report.html`
- Resumo de KPI: `data/gold/kpi_summary.json`
- CSVs de saída: `data/gold/exports/*.csv`

## Ver Resultado no DuckDB UI

Abra o arquivo correto:

```bash
duckdb -ui data/lakehouse.duckdb
```

No editor SQL:

```sql
SHOW ALL TABLES;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
ORDER BY 1,2;
```

## Comandos Úteis

Entrar no container do Airflow:

```bash
docker-compose exec airflow-scheduler bash
```

Desligar tudo:

```bash
docker-compose down
```

Limpar recursos Docker não usados:

```bash
docker system prune -f
```

## Troubleshooting Rápido

- As DAGs não aparecem na UI
  - Garanta que o scheduler está rodando:
  - `docker-compose up -d airflow-scheduler`

- Erro `ModuleNotFoundError: duckdb` nas tasks do Airflow
  - Faça build e recrie os serviços:
  - `docker-compose build airflow-webserver airflow-scheduler`
  - `docker-compose up -d --force-recreate airflow-webserver airflow-scheduler`

- Erro de permissão em `/app/data`
  - Recrie os serviços com o compose atual:
  - `docker-compose up -d --force-recreate airflow-webserver airflow-scheduler`
