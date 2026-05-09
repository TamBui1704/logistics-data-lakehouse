"""
airflow/dags/logistics_etl_dag.py
==================================
DAG điều phối pipeline ETL logistics chạy hàng ngày lúc 02:00 (GMT+7).

Pipeline:
  1. data_ingestion  → Sinh dữ liệu giả lập và đẩy lên S3 (Bronze layer)
  2. dbt_run         → Chạy tất cả dbt models (Bronze → Silver → Gold)
  3. dbt_test        → Kiểm tra chất lượng dữ liệu bằng dbt tests
  4. notify_success  → Log kết quả pipeline hoàn thành

Lịch chạy: Hàng ngày lúc 02:00 (UTC+7 = 19:00 UTC hôm trước)
"""

from __future__ import annotations

import os
import subprocess
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ── Constants ─────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.environ["DBT_PROJECT_DIR"]
DUCKDB_PATH     = os.environ["DUCKDB_PATH"]
S3_ENDPOINT     = os.environ["S3_ENDPOINT_URL"]
DATA_GEN_DIR    = "/opt/airflow/data_gen"
ROWS_PER_DAY    = 2000

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":             "logistics-team",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id              = "logistics_daily_etl",
    description         = "ETL pipeline hàng ngày: Ingest → dbt (Bronze→Silver→Gold) → Test",
    default_args        = default_args,
    # Chạy lúc 02:00 GMT+7 = 19:00 UTC
    schedule_interval   = "0 19 * * *",
    start_date          = days_ago(1),
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["logistics", "etl", "dbt", "duckdb"],
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # Task 1: Data Ingestion
    # Chạy Python script để sinh dữ liệu và upload lên S3 (Bronze)
    # ─────────────────────────────────────────────────────────────────────────
    def run_data_ingestion(**context):
        """Sinh dữ liệu logistics và đẩy lên S3 Bronze layer."""
        import importlib.util
        import sys

        # Lấy ngày chạy từ Airflow execution date
        logical_date = context["logical_date"].date()

        # Load module data_gen/main.py
        spec = importlib.util.spec_from_file_location(
            "data_gen_main", f"{DATA_GEN_DIR}/main.py"
        )
        data_gen = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(data_gen)

        # Chạy hàm chính
        data_gen.run(target_date=logical_date, n_rows=ROWS_PER_DAY)

        return f"Ingested {ROWS_PER_DAY} rows for {logical_date}"

    task_ingest = PythonOperator(
        task_id         = "data_ingestion",
        python_callable = run_data_ingestion,
        provide_context = True,
        doc_md          = """
        ### Data Ingestion
        Sử dụng `data_gen/main.py` để sinh dữ liệu logistics giả lập (Faker)
        và upload CSV lên Floci S3 trong thư mục `bronze/shipments/date=YYYY-MM-DD/`.
        """,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Task 2: dbt run (Bronze → Silver → Gold)
    # ─────────────────────────────────────────────────────────────────────────
    task_dbt_run = BashOperator(
        task_id  = "dbt_run",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--vars '{{\"data_date\": \"{{{{ ds }}}}\"}}' "
        ),
        env=os.environ.copy(),
        doc_md = """
        ### dbt Run
        Chạy toàn bộ dbt models:
        - **Bronze**: `stg_shipments`, `stg_carriers`, `stg_routes` (view đọc từ S3)
        - **Silver**: `int_shipment_details` (table – enrich + clean)
        - **Gold**: `fct_logistics_kpis` (table – KPI theo ngày + carrier)
        """,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3: dbt test (Data Quality)
    # ─────────────────────────────────────────────────────────────────────────
    task_dbt_test = BashOperator(
        task_id  = "dbt_test",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
        ),
        env=os.environ.copy(),
        doc_md = """
        ### dbt Test
        Chạy các bài test chất lượng dữ liệu đã định nghĩa trong `schema.yml`:
        - Unique / Not null keys
        - Referential integrity (carrier_id)
        - Accepted values (status)
        """,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Task 4: Notify Success
    # ─────────────────────────────────────────────────────────────────────────
    def notify_success(**context):
        """Log kết quả pipeline hoàn thành."""
        logical_date = context["logical_date"].date()
        print("=" * 60)
        print(f"✅ Logistics ETL pipeline hoàn thành!")
        print(f"   Ngày xử lý : {logical_date}")
        print(f"   DuckDB file: {DUCKDB_PATH}")
        print(f"   S3 endpoint: {S3_ENDPOINT}")
        print("=" * 60)
        print("Truy vấn kết quả:")
        print("  duckdb /opt/airflow/data/logistics.duckdb")
        print("  SELECT * FROM gold.fct_logistics_kpis LIMIT 10;")

    task_notify = PythonOperator(
        task_id         = "notify_success",
        python_callable = notify_success,
        provide_context = True,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    task_ingest >> task_dbt_run >> task_dbt_test >> task_notify
