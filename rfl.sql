
SELECT 
customer_unique_id, 
COUNT(DISTINCT order_id) AS counts_orders, 
MAX (order_purchase_timestamp) AS last_date, 
SUM(price) as orders_sum_price
FROM orders 
JOIN customers USING (customer_id)
JOIN order_items USING(order_id)
WHERE order_status = 'delivered'
GROUP BY customer_unique_id
ORDER BY counts_orders desc