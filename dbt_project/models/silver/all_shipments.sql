-- models/silver/all_shipments.sql
-- ============================================================
-- View tổng hợp toàn bộ dữ liệu Silver từ S3
-- ============================================================

-- depends_on: {{ ref('int_shipment_details') }}

{{
  config(
    materialized = 'view',
    schema       = 'silver'
  )
}}

SELECT *
FROM read_parquet(
    's3://logistics-lake/silver/**/*.parquet',
    hive_partitioning = true
)
