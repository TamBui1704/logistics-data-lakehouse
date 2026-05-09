-- models/bronze/stg_carriers.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô về công ty vận chuyển từ S3
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

SELECT
    carrier_id,
    carrier_name,
    carrier_type,
    max_weight_kg,
    CAST(rating AS DOUBLE) AS rating,
    CAST(is_active AS BOOLEAN) AS is_active,
    CAST(created_at AS DATE) AS created_at,
    -- Metadata
    current_timestamp AS _loaded_at
FROM read_csv_auto(
    's3://logistics-lake/bronze/carriers/carriers.csv',
    header = true,
    nullstr = 'NULL'
)
