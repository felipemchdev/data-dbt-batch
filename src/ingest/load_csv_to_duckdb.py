from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pandas as pd


DATA_DIR = Path("/data")
DUCKDB_PATH = DATA_DIR / "lakehouse.duckdb"


def normalize_table_name(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return "table_data"
    if normalized[0].isdigit():
        return f"t_{normalized}"
    return normalized


def main() -> None:
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {DATA_DIR}")

    connection = duckdb.connect(str(DUCKDB_PATH))
    try:
        for csv_file in csv_files:
            table_name = normalize_table_name(csv_file.stem)
            dataframe = pd.read_csv(csv_file)
            connection.register("csv_frame", dataframe)
            connection.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM csv_frame'
            )
            connection.unregister("csv_frame")
            print(f"{csv_file.name} -> {table_name} ({len(dataframe)} linhas)")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
