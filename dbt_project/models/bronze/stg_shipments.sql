-- models/bronze/stg_shipments.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô về đơn hàng từ S3
-- Dữ liệu được partition theo date=YYYY-MM-DD
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

SELECT
    shipment_id,
    carrier_id,
    route_id,
    product_type,
    vehicle_type,
    CAST(weight_kg AS DOUBLE) AS weight_kg,
    CAST(shipping_fee AS DOUBLE) AS shipping_fee,
    status,
    sender_name,
    receiver_name,
    CAST(NULLIF(TRIM(created_at), '') AS TIMESTAMP) AS created_at,
    CAST(NULLIF(TRIM(delivered_at), '') AS TIMESTAMP) AS delivered_at,
    CAST(data_date AS DATE) AS data_date,
    -- Metadata
    current_timestamp AS _loaded_at
FROM read_csv_auto(
    's3://logistics-lake/bronze/shipments/**/*.csv',
    header    = true,
    nullstr   = 'NULL',
    union_by_name = true,
    hive_partitioning = true,
    types = {'created_at': 'VARCHAR', 'delivered_at': 'VARCHAR'}
)
