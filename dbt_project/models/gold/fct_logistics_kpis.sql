-- models/gold/fct_logistics_kpis.sql
-- ============================================================
-- Gold Layer: KPI tổng hợp theo ngày + carrier
-- Lưu trữ: Parquet on S3 (Partitioned by date)
-- ============================================================

{{
  config(
    materialized = 'external',
    location     = 's3://logistics-lake/gold/data_date=' ~ var('data_date') ~ '/kpis.parquet',
    format       = 'parquet'
  )
}}

WITH base AS (
    -- Chỉ tính toán cho dữ liệu ngày hiện tại
    SELECT * FROM {{ ref('int_shipment_details') }}
    WHERE data_date = CAST('{{ var("data_date") }}' AS DATE)
)

SELECT
    data_date,
    carrier_id,
    carrier_name,
    carrier_type,

    -- ── Khối lượng ─────────────────────────────────────────────────────
    COUNT(shipment_id)                                          AS total_shipments,
    COUNT(CASE WHEN status = 'Đã giao' THEN 1 END)             AS delivered_count,
    COUNT(CASE WHEN status = 'Hoàn hàng' THEN 1 END)           AS returned_count,
    COUNT(CASE WHEN status = 'Thất lạc' THEN 1 END)            AS lost_count,

    -- ── Tỷ lệ ──────────────────────────────────────────────────────────
    ROUND(
        COUNT(CASE WHEN status = 'Đã giao' THEN 1 END) * 100.0 / COUNT(shipment_id), 2
    )                                                           AS delivery_rate_pct,

    ROUND(
        COUNT(CASE WHEN is_on_time = TRUE THEN 1 END) * 100.0
        / NULLIF(COUNT(CASE WHEN status = 'Đã giao' THEN 1 END), 0), 2
    )                                                           AS on_time_rate_pct,

    -- ── Doanh thu & Trọng lượng ─────────────────────────────────────────
    ROUND(SUM(shipping_fee), 0)                                 AS total_revenue,
    ROUND(AVG(shipping_fee), 0)                                 AS avg_revenue_per_shipment,
    ROUND(SUM(weight_kg), 2)                                    AS total_weight_kg,
    ROUND(AVG(weight_kg), 2)                                    AS avg_weight_kg,

    -- ── Thời gian vận chuyển ────────────────────────────────────────────
    ROUND(AVG(actual_transit_days), 1)                          AS avg_transit_days,
    ROUND(AVG(distance_km), 0)                                  AS avg_distance_km,
    ROUND(AVG(fee_per_km), 0)                                   AS avg_fee_per_km,

    -- ── Rating ─────────────────────────────────────────────────────────
    MAX(carrier_rating)                                         AS carrier_rating,

    -- ── Breakdown theo loại sản phẩm (JSON) ────────────────────────────
    COUNT(CASE WHEN product_type = 'Điện tử'    THEN 1 END)    AS cnt_electronics,
    COUNT(CASE WHEN product_type = 'Thực phẩm'  THEN 1 END)    AS cnt_food,
    COUNT(CASE WHEN product_type = 'Quần áo'    THEN 1 END)    AS cnt_clothing,
    COUNT(CASE WHEN product_type = 'Mỹ phẩm'   THEN 1 END)    AS cnt_cosmetics,
    COUNT(CASE WHEN product_type = 'Đồ gia dụng' THEN 1 END)   AS cnt_household,

    current_timestamp                                           AS _updated_at

FROM base
GROUP BY
    data_date,
    carrier_id,
    carrier_name,
    carrier_type
