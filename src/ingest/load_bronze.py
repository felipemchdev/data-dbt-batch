from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from src.utils.config import ensure_directories, resolve_raw_file, settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "products",
    "sellers",
    "geolocation",
]


def load_table_to_bronze(con: duckdb.DuckDBPyConnection, table: str) -> None:
    source_path = resolve_raw_file(table)
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    logger.info("Carregando tabela bronze.%s a partir de %s", table, source_path)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE bronze.{table} AS
        SELECT
            *,
            CAST('{ingestion_ts}' AS TIMESTAMP) AS ingestion_timestamp,
            '{source_path.name}' AS source_file
        FROM read_csv_auto('{source_path.as_posix()}', HEADER=TRUE, SAMPLE_SIZE=-1);
        """
    )

    output_path = settings.bronze_dir / f"{table}.parquet"
    if output_path.exists():
        output_path.unlink()

    df = con.execute(f"SELECT * FROM bronze.{table}").fetch_df()
    df.to_parquet(output_path, index=False)
    logger.info("Parquet bronze salvo em %s", output_path)


def main() -> None:
    ensure_directories()
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        for table in TABLES:
            load_table_to_bronze(con, table)
    finally:
        con.close()

    logger.info("Carga bronze concluida com sucesso")


if __name__ == "__main__":
    main()
