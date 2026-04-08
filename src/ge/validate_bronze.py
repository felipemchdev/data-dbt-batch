from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import great_expectations as ge
import pandas as pd

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "customers": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_estimated_delivery_date",
    ],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    "order_payments": ["order_id", "payment_type", "payment_value"],
    "products": ["product_id"],
    "sellers": ["seller_id", "seller_city", "seller_state"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
}

ID_COLUMNS: Dict[str, List[str]] = {
    "customers": ["customer_id"],
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_id", "product_id", "seller_id"],
    "order_payments": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
}

TABLES = list(REQUIRED_COLUMNS.keys())


def run_expectations(table: str, file_path: Path) -> list[str]:
    logger.info("Validando tabela %s (%s)", table, file_path)
    df = pd.read_parquet(file_path)
    gdf = ge.from_pandas(df)
    failures: list[str] = []

    for column in REQUIRED_COLUMNS[table]:
        result = gdf.expect_column_to_exist(column)
        if not result.success:
            failures.append(f"[{table}] coluna obrigatoria ausente: {column}")

    for column in ID_COLUMNS[table]:
        result = gdf.expect_column_values_to_not_be_null(column)
        if not result.success:
            failures.append(f"[{table}] coluna ID com nulos: {column}")

    if table == "order_payments" and "payment_value" in df.columns:
        result = gdf.expect_column_values_to_be_between("payment_value", min_value=0)
        if not result.success:
            failures.append("[order_payments] payment_value possui valor negativo")

    if table == "order_items":
        if "price" in df.columns:
            result = gdf.expect_column_values_to_be_between("price", min_value=0)
            if not result.success:
                failures.append("[order_items] price possui valor negativo")
        if "freight_value" in df.columns:
            result = gdf.expect_column_values_to_be_between("freight_value", min_value=0)
            if not result.success:
                failures.append("[order_items] freight_value possui valor negativo")

    if table == "geolocation":
        if "geolocation_lat" in df.columns:
            result = gdf.expect_column_values_to_be_between("geolocation_lat", min_value=-90, max_value=90)
            if not result.success:
                failures.append("[geolocation] latitude fora do intervalo [-90, 90]")
        if "geolocation_lng" in df.columns:
            result = gdf.expect_column_values_to_be_between("geolocation_lng", min_value=-180, max_value=180)
            if not result.success:
                failures.append("[geolocation] longitude fora do intervalo [-180, 180]")

    return failures


def main() -> None:
    all_failures: list[str] = []

    for table in TABLES:
        parquet_path = settings.bronze_dir / f"{table}.parquet"
        if not parquet_path.exists():
            all_failures.append(f"Arquivo bronze nao encontrado para {table}: {parquet_path}")
            continue
        all_failures.extend(run_expectations(table, parquet_path))

    if all_failures:
        logger.error("Quality gate falhou com %s problema(s)", len(all_failures))
        for failure in all_failures:
            logger.error(" - %s", failure)
        raise SystemExit(1)

    logger.info("Quality gate aprovado para todas as tabelas bronze")


if __name__ == "__main__":
    main()
