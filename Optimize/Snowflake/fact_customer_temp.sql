-- 7 secs

SELECT DISTINCT customer_key, '1' AS temp_customer
FROM src_snowflake.fact_order;

-- 3 secs

SELECT customer_key, '1' AS temp_customer
FROM src_snowflake.fact_order
GROUP BY customer_key;