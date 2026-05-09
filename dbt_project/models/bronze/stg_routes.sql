-- models/bronze/stg_routes.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô về tuyến đường từ S3
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

SELECT
    route_id,
    origin_province,
    dest_province,
    CAST(distance_km AS INTEGER) AS distance_km,
    CAST(est_transit_days AS INTEGER) AS est_transit_days,
    -- Metadata
    current_timestamp AS _loaded_at
FROM read_csv_auto(
    's3://logistics-lake/bronze/routes/routes.csv',
    header = true,
    nullstr = 'NULL'
)
