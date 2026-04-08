from __future__ import annotations

import json
from datetime import datetime, timezone

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


def main() -> None:
    ensure_directories()
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }

    logger.info("Validando presenca dos CSVs no diretorio raw: %s", settings.raw_dir)
    for table in TABLES:
        csv_path = resolve_raw_file(table)
        manifest["tables"][table] = {
            "source_file": csv_path.name,
            "path": str(csv_path.resolve()),
            "size_bytes": csv_path.stat().st_size,
        }
        logger.info("Tabela '%s' localizada em %s", table, csv_path)

    manifest_content = json.dumps(manifest, indent=2)
    primary_manifest_path = settings.raw_dir / "raw_manifest.json"

    try:
        primary_manifest_path.write_text(manifest_content, encoding="utf-8")
        logger.info("Manifest raw atualizado em %s", primary_manifest_path)
        return
    except PermissionError:
        logger.warning(
            "Sem permissao para gravar manifest em %s. Tentando fallback no diretorio de logs.",
            primary_manifest_path,
        )

    fallback_manifest_path = settings.logs_dir / "raw_manifest.json"
    try:
        fallback_manifest_path.write_text(manifest_content, encoding="utf-8")
        logger.info("Manifest raw atualizado em %s", fallback_manifest_path)
    except PermissionError:
        logger.warning(
            "Sem permissao para gravar manifest em %s. Seguindo sem persistir o manifest.",
            fallback_manifest_path,
        )


if __name__ == "__main__":
    main()
