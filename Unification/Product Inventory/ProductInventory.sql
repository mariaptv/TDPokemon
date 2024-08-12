CREATE TABLE IF NOT EXISTS rr_tpci_stg.product_inventory (
  time bigint,
  id VARCHAR,
  storefront VARCHAR,
  storefront_2_char_format VARCHAR,
  sku VARCHAR,
  quantity VARCHAR
) with (
  bucketed_on = array ['id'],
  bucket_count = 512
);
INSERT INTO rr_tpci_stg.product_inventory
SELECT time as time,
  id AS id,
  NULLIF(TRIM(storefront), '') AS storefront,
  NULLIF(TRIM(storefront_2_char_format), '') AS storefront_2_char_format,
  NULLIF(TRIM(sku), '') AS sku,
  NULLIF(TRIM(CAST (quantity AS VARCHAR)), '') AS quantity
FROM (
    SELECT TD_TIME_PARSE(update_date) as time,
      to_base64url(xxhash64(cast(coalesce(sku, '') as varbinary))) as id,
      storefront,
      CASE WHEN storefront ='pokemoncenter.com' THEN 'US' WHEN storefront ='pokemoncenter.com/en-gb'  THEN 'CA' WHEN storefront ='pokemoncenter.com/en-ca'  THEN 'UK' ELSE storefront  END    AS  storefront_2_char_format,
      sku ,
      quantity
    FROM src_snowflake.inventory_ats_current
    where time > $ { td.last_results.last_session_time }
      and to_base64url(xxhash64(cast(coalesce(sku, '') as varbinary))) not in (
        select id
        from rr_tpci_stg.product_inventory
        where id is not null
      )
      and to_base64url(xxhash64(cast(coalesce(sku, '') as varbinary))) is not null
    group by sku
  )