-- models/silver/int_shipment_details.sql
-- ============================================================
-- Silver Layer: Làm sạch và JOIN các bảng thô
-- Lưu trữ: Parquet on S3 (Partitioned by date)
-- ============================================================

{{
  config(
    materialized = 'external',
    location     = 's3://logistics-lake/silver/data_date=' ~ var('data_date') ~ '/shipments.parquet',
    format       = 'parquet'
  )
}}

WITH shipments AS (
    -- Chỉ lấy dữ liệu của ngày đang chạy để thực hiện nạp incremental (partitioned)
    SELECT * FROM {{ ref('stg_shipments') }}
    WHERE data_date = CAST('{{ var("data_date") }}' AS DATE)
),

carriers AS (
    SELECT * FROM {{ ref('stg_carriers') }}
),

routes AS (
    SELECT * FROM {{ ref('stg_routes') }}
),

enriched AS (
    SELECT
        -- Shipment info
        s.shipment_id,
        s.data_date,
        s.created_at,
        s.delivered_at,
        s.status,
        s.product_type,
        s.vehicle_type,
        s.weight_kg,
        s.shipping_fee,
        s.sender_name,
        s.receiver_name,

        -- Carrier info
        c.carrier_id,
        c.carrier_name,
        c.carrier_type,
        c.rating AS carrier_rating,

        -- Route info
        r.route_id,
        r.origin_province,
        r.dest_province,
        r.distance_km,
        r.est_transit_days,

        -- Calculated fields
        CASE
            WHEN s.delivered_at IS NOT NULL
            THEN DATE_DIFF('day', s.created_at, s.delivered_at)
        END AS actual_transit_days,

        CASE
            WHEN s.delivered_at IS NOT NULL AND DATE_DIFF('day', s.created_at, s.delivered_at) <= r.est_transit_days
            THEN TRUE ELSE FALSE
        END AS is_on_time,

        -- Revenue per km
        CASE
            WHEN r.distance_km > 0
            THEN ROUND(s.shipping_fee / r.distance_km, 0)
        END AS fee_per_km,

        -- Metadata
        s._loaded_at

    FROM shipments s
    LEFT JOIN carriers c ON s.carrier_id = c.carrier_id
    LEFT JOIN routes   r ON s.route_id   = r.route_id
)

SELECT * FROM enriched
