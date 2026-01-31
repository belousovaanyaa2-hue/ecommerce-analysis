
UPDATE orders
SET order_purchase_timestamp  = DATE(order_purchase_timestamp);
with t1 as 
(select
customer_unique_id, 
DATE(order_purchase_timestamp) as order_date, 
min(DATE(order_purchase_timestamp)) over (partition by customer_unique_id) as start_date, 
count (customer_id) over (partition by customer_unique_id) as orders_count
FROM orders 
join customers USING (customer_id)
WHERE order_status = 'delivered'
),
t2 as (select 
customer_unique_id, orders_count, 
strftime('%Y-%m', order_date) AS order_month, 
strftime ('%Y-%m', start_date) as start_month
from t1 
),
t3 as (SELECT 
start_month, 
order_month, 
    (CAST(strftime('%Y', order_month || '-01') AS INTEGER) - CAST(strftime('%Y', start_month || '-01') AS INTEGER)) * 12
  + (CAST(strftime('%m', order_month || '-01') AS INTEGER) - CAST(strftime('%m', start_month || '-01') AS INTEGER)) AS months_diff,
CAST(COUNT (DISTINCT customer_unique_id)  AS REAL)
    / MAX(COUNT (DISTINCT customer_unique_id)) OVER (PARTITION BY start_month) * 100 AS retention
FROM t2
GROUP BY 1, 2 
ORDER BY start_month, order_month )
SELECT 
   start_month, 
   order_month, 
   ROUND(retention, 2) as retention 
FROM t3
where months_diff = 1 
