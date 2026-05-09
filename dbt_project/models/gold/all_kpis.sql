-- models/gold/all_kpis.sql
-- ============================================================
-- View tổng hợp toàn bộ dữ liệu Gold từ S3
-- ============================================================

-- depends_on: {{ ref('fct_logistics_kpis') }}

{{
  config(
    materialized = 'view',
    schema       = 'gold'
  )
}}

SELECT *
FROM read_parquet(
    's3://logistics-lake/gold/**/*.parquet',
    hive_partitioning = true
)
