from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    root_dir: Path = ROOT_DIR
    data_dir: Path = ROOT_DIR / "data"
    raw_dir: Path = data_dir / "raw"
    bronze_dir: Path = data_dir / "bronze"
    silver_dir: Path = data_dir / "silver"
    gold_dir: Path = data_dir / "gold"
    exports_dir: Path = gold_dir / "exports"
    logs_dir: Path = ROOT_DIR / "logs"
    duckdb_path: Path = data_dir / "lakehouse.duckdb"

    table_candidates: Dict[str, List[str]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "table_candidates",
            {
                "customers": ["customers.csv", "olist_customers_dataset.csv"],
                "orders": ["orders.csv", "olist_orders_dataset.csv"],
                "order_items": ["order_items.csv", "olist_order_items_dataset.csv"],
                "order_payments": ["order_payments.csv", "olist_order_payments_dataset.csv"],
                "products": ["products.csv", "olist_products_dataset.csv"],
                "sellers": ["sellers.csv", "olist_sellers_dataset.csv"],
                "geolocation": ["geolocation.csv", "olist_geolocation_dataset.csv"],
            },
        )


settings = Settings()


def ensure_directories() -> None:
    for path in [
        settings.raw_dir,
        settings.bronze_dir,
        settings.silver_dir,
        settings.gold_dir,
        settings.exports_dir,
        settings.logs_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def resolve_raw_file(table_name: str) -> Path:
    candidates = settings.table_candidates[table_name]
    for candidate in candidates:
        file_path = settings.raw_dir / candidate
        if file_path.exists():
            return file_path
    raise FileNotFoundError(
        f"Arquivo CSV da tabela '{table_name}' nao encontrado em {settings.raw_dir}. "
        f"Nomes aceitos: {candidates}"
    )
