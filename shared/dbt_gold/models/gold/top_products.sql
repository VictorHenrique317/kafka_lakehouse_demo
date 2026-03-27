{{ config(materialized='table', file_format='delta') }}

SELECT
    product_id,
    product_name,
    product_category,
    SUM(total_amount)  AS revenue,
    SUM(quantity)      AS units_sold,
    COUNT(order_id)    AS order_count
FROM {{ source('silver', 'orders') }}
GROUP BY product_id, product_name, product_category
ORDER BY revenue DESC
