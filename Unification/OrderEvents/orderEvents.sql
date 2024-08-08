CREATE TABLE campaign_events AS
SELECT
TD_TIME_PARSE(order_timestamp_utc) as time, 
customer_key,
order_id,
order_item_id,
product_key,
order_status,
storefront
FROM src_snowflake.fact_order