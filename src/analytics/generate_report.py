from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb
import pandas as pd

from src.utils.config import ensure_directories, settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def query_df(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    return con.execute(query).fetch_df()


def resolve_schema_for_table(
    con: duckdb.DuckDBPyConnection, table_name: str, schema_candidates: list[str]
) -> str:
    for schema_name in schema_candidates:
        exists = con.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema_name, table_name],
        ).fetchone()[0]
        if exists:
            return schema_name

    raise RuntimeError(
        f"Tabela '{table_name}' nao encontrada nos schemas candidatos: {schema_candidates}"
    )


def build_html_report(total_revenue: float, total_orders: int, avg_delivery_days: float) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""
<!DOCTYPE html>
<html lang=\"pt-BR\">
<head>
  <meta charset=\"UTF-8\" />
  <title>Lakehouse Olist - KPI Report</title>
  <style>
    body {{ font-family: 'Segoe UI', sans-serif; margin: 32px; background: #f6f8fb; color: #1f2937; }}
    .card {{ background: #fff; border-radius: 12px; padding: 24px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); max-width: 680px; }}
    h1 {{ margin-top: 0; }}
    .metric {{ margin: 14px 0; font-size: 18px; }}
    .label {{ font-weight: 700; }}
    .footer {{ margin-top: 20px; font-size: 12px; color: #4b5563; }}
  </style>
</head>
<body>
  <div class=\"card\">
    <h1>Relatorio Executivo - Olist Lakehouse</h1>
    <div class=\"metric\"><span class=\"label\">Total Revenue:</span> R$ {total_revenue:,.2f}</div>
    <div class=\"metric\"><span class=\"label\">Total Orders:</span> {total_orders:,}</div>
    <div class=\"metric\"><span class=\"label\">Media de Entrega:</span> {avg_delivery_days:.2f} dias</div>
    <div class=\"footer\">Gerado em: {generated_at}</div>
  </div>
</body>
</html>
""".strip()


def main() -> None:
    ensure_directories()

    con = duckdb.connect(str(settings.duckdb_path))
    try:
        gold_schema = resolve_schema_for_table(
            con,
            "mart_revenue_daily",
            ["gold", "main_gold"],
        )
        silver_schema = resolve_schema_for_table(
            con,
            "stg_orders_enriched",
            ["silver", "main_silver"],
        )

        revenue_df = query_df(
            con,
            f"SELECT COALESCE(SUM(revenue), 0) AS total_revenue FROM {gold_schema}.mart_revenue_daily",
        )
        orders_df = query_df(
            con,
            f"SELECT COUNT(DISTINCT order_id) AS total_orders FROM {silver_schema}.stg_orders_enriched",
        )
        delivery_df = query_df(
            con,
            f"SELECT COALESCE(AVG(avg_delivery_time_days), 0) AS avg_delivery_days FROM {gold_schema}.mart_delivery_performance",
        )

        total_revenue = float(revenue_df.iloc[0]["total_revenue"])
        total_orders = int(orders_df.iloc[0]["total_orders"])
        avg_delivery_days = float(delivery_df.iloc[0]["avg_delivery_days"])

        report_html = build_html_report(total_revenue, total_orders, avg_delivery_days)
        report_path = settings.gold_dir / "report.html"
        report_path.write_text(report_html, encoding="utf-8")
        logger.info("Relatorio HTML gerado em %s", report_path)

        marts = [
            "mart_revenue_daily",
            "mart_top_products",
            "mart_customer_metrics",
            "mart_seller_performance",
            "mart_delivery_performance",
        ]
        for mart in marts:
            df = query_df(con, f"SELECT * FROM {gold_schema}.{mart}")
            export_path = settings.exports_dir / f"{mart}.csv"
            df.to_csv(export_path, index=False)
            logger.info("Export dashboard pronto: %s", export_path)

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "avg_delivery_days": avg_delivery_days,
            "report_path": str(report_path.resolve()),
        }
        summary_path = settings.gold_dir / "kpi_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logger.info("Resumo de KPIs salvo em %s", summary_path)

    finally:
        con.close()


if __name__ == "__main__":
    main()
