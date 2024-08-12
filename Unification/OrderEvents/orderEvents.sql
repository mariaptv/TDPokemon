CREATE TABLE IF NOT EXISTS rr_tpci_stg.oder_events (
    time BIGINT,
    id VARCHAR,
    customer_key VARCHAR,
    order_id VARCHAR,
    order_item_id VARCHAR,
    product_key VARCHAR,
    order_status VARCHAR,
    storefront VARCHAR,
    demand_quantity VARCHAR,
    ordered_product_quantity VARCHAR
) with (
    bucketed_on = array ['id'],
    bucket_count = 512
);
insert into rr_tpci_stg.oder_events
SELECT min(time) as time,
    id as id,
    max(customer_key) as customer_key,
    max(order_id) as order_id,
    max(order_item_id) as order_item_id,
    max(product_key) as product_key,
    max(order_status) as order_status,
    max(storefront) as storefront,
    max(demand_quantity) as demand_quantity,
    max(ordered_product_quantity) as ordered_product_quantity
FROM (
        SELECT min(TD_TIME_PARSE(order_timestamp_utc)) as time,
  max(to_base64url(xxhash64(cast(coalesce(CAST(order_id as varchar),'') || coalesce(cast(customer_key as varchar),'') as varbinary)))) as id,
            customer_key as customer_key,
            order_id as order_id,
            max(order_item_id) as order_item_id,
            max(product_key) as product_key,
            max(order_status) as order_status,
            max(storefront) as storefront,
            max(demand_quantity) as demand_quantity,
            max(ordered_product_quantity) as ordered_product_quantity
            FROM src_snowflake.fact_order
            where
    time > ${td.last_results.last_session_time} and 
    to_base64url(xxhash64(cast(coalesce(cast(order_id as varchar),'') || coalesce(customer_key,'') as varbinary))) not in (select id from rr_tpci_stg.oder_events where id is not null)
    and to_base64url(xxhash64(cast(cast(coalesce(cast(order_id as varchar),'') ||  coalesce(customer_key,'') as varbinary))) is not null
group by order_id, customer_key
    ))