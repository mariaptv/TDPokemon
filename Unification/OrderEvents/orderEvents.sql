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
SELECT time AS time,
    id,
    CAST(customer_key AS VARCHAR) AS customer_key,
    NULLIF(TRIM(order_id), '') AS order_id,
    NULLIF(TRIM(order_item_id), '') AS order_item_id,
    NULLIF(TRIM(CAST(product_key AS VARCHAR)), '') AS product_key,
    NULLIF(TRIM(order_status), '') AS order_status,
    NULLIF(TRIM(storefront), '') AS storefront,
    NULLIF(TRIM(CAST(demand_quantity AS VARCHAR)), '') AS demand_quantity,
    NULLIF(
        TRIM(CAST(ordered_product_quantity AS VARCHAR)),
        ''
    ) AS ordered_product_quantity
FROM (
        SELECT TD_TIME_PARSE(order_timestamp_utc) AS time,
            to_base64url(
                xxhash64(
                    cast(
                        coalesce(CAST(order_id AS varchar), '') || coalesce(CAST(customer_key AS VARCHAR), '') AS varbinary
                    )
                )
            ) as id,
            customer_key AS customer_key,
            order_id AS order_id,
            order_item_id AS order_item_id,
            product_key AS product_key,
            order_status AS order_status,
            CASE
                WHEN storefront = 'www.pokemoncenter.com' OR storefront = 'pokemoncenter.com' THEN 'US'
                WHEN storefront = 'www.pokemoncenter.com/en-gb' OR storefront = 'pokemoncenter.com/en-gb' THEN 'UK'
                WHEN storefront = 'www.pokemoncenter.com/en-ca' OR storefront = 'www.pokemoncenter.ca' THEN 'CA'
                ELSE storefront
            END AS storefront,
            demand_quantity AS demand_quantity,
            ordered_product_quantity AS ordered_product_quantity
        FROM src_snowflake.fact_order
        WHERE time > $ { td.last_results.last_session_time }
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(CAST(order_id AS VARCHAR), '') || coalesce(CAST(customer_key AS VARCHAR), '') as varbinary
                    )
                )
            ) not in (
                select id
                from rr_tpci_stg.oder_events
                where id is not null
            )
            and to_base64url(
                xxhash64(
                    cast(
                        cast(
                            coalesce(cast(order_id as varchar), '') || coalesce(customer_key, '') as varbinary
                        )
                    )
                ) is not null
            )
    )