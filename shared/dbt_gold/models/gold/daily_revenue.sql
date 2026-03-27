{{ config(materialized='table', file_format='delta') }}

SELECT
    order_date,
    currency,
    SUM(total_amount)         AS revenue,
    COUNT(DISTINCT order_id)  AS order_count,
    AVG(total_amount)         AS avg_order_value
FROM {{ source('silver', 'orders') }}
GROUP BY order_date, currency
ORDER BY order_date DESC, revenue DESC
