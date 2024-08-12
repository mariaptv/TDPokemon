CREATE TABLE IF NOT EXISTS rr_tpci_stg.product_inventory (
    time bigint,
    id VARCHAR,
    storefront VARCHAR,
    sku VARCHAR,
    quantity VARCHAR
)

with (
  bucketed_on = array['id'],
  bucket_count = 512
);

INSERT INTO rr_tpci_stg.pro
SELECT min(time) as time,
     id as id,
     min(storefront) AS storefront,
    min(sku),
    min(quantity)
FROM  (
    SELECT
    min(TD_TIME_PARSE(update_date)) as time,
  max(to_base64url(xxhash64(cast(coalesce(sku,'') as varbinary)))) as id,
  max(sku),
  max(quantity)
  FROM
    src_snowflake.inventory_ats_current
    where
    time > ${td.last_results.last_session_time} and 
    to_base64url(xxhash64(cast(coalesce(sku,'') as varbinary))) not in (select id from rr_tpci_stg.product_inventory where id is not null)
    and to_base64url(xxhash64(cast(coalesce(sku,'') as varbinary))) is not null
group by sku

)