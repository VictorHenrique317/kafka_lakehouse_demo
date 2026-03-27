{{ config(materialized='table', file_format='delta') }}

SELECT
    order_date,
    order_status,
    COUNT(*)             AS order_count,
    SUM(total_amount)    AS total_revenue
FROM {{ source('silver', 'orders') }}
GROUP BY order_date, order_status
ORDER BY order_date DESC, order_count DESC
