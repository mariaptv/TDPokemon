-- Ask exception list for email
-- Need to create a standardized event name for each event like pageview, click, add to cart, etc.
-- Should we assume some atributes as the same or try to find them? Check if uuid from amplitude matches customer_uid. If so, map. If no, new field.
-- I dont understand this: All TD JS SDK fields. Map fields from Amplitude and EP and keep the td_ prefix
-- SELECT c.customer_uid AS c, a.uuid AS a
-- FROM src_elastic_path_data.cart c
-- JOIN src_amplitude_pokemoncenter.amplitude_pokemoncenter_prod a
-- ON c.customer_uid = a.user_id;

CREATE TABLE IF NOT EXISTS rr_tpci_stg.clicstream_events(
    time bigint,
    id varchar,
    cp_email VARCHAR,
    customer_uid varchar,
    storecode varchar,
    cart_status varchar,
    shopping_cart_uid varchar,
    sku_code varchar,
    currency varchar,
    list_unit_price varchar,
    quantity varchar,
    item_type varchar,
    isfreegiftitem varchar,
    cart_item_creation_date varchar,
    shipping_option_code varchar,
    cp_first_name varchar,
    cp_last_name varchar,
    cp_phone varchar,
    billing_first_name varchar,
    billing_last_name varchar,
    billing_street_1 varchar,
    billing_city varchar,
    billing_sub_country varchar,
    billing_country varchar,
    payment_type_name varchar,
    promo_unit_price varchar,
    discount_amount varchar
) with (
    bucketed_on = array ['id'],
    bucket_count = 512
);
insert into rr_tpci_stg.clicstream_events
select min(time) as time,
    min(time) as min_insert_timestamp,
    min(min_update_timestamp) as min_update_timestamp,
    id as id,
    max(cp_email) as cp_email,
    customer_uid as customer_uid,
    max(storecode) as storecode,
    max(cart_status) as cart_status,
    shopping_cart_uid as shopping_cart_uid,
    sku_code as sku_code,
    max(currency) as currency,
    max(list_unit_price) as list_unit_price,
    max(quantity) as quantity,
    max(item_type) as item_type,
    max(isfreegiftitem) as isfreegiftitem,
    max(cart_item_creation_date) as cart_item_creation_date,
    max(shipping_option_code) as shipping_option_code,
    max(cp_first_name) as cp_first_name,
    max(cp_last_name) as cp_last_name,
    max(cp_phone) as cp_phone,
    max(phone_std) as phone_std,
    max(billing_first_name) as billing_first_name,
    max(billing_last_name) as billing_last_name,
    max(billing_street_1) as billing_street_1,
    max(billing_city) as billing_city,
    max(billing_sub_country) as billing_sub_country,
    max(billing_country) as billing_country,
    max(payment_type_name) as payment_type_name,
    max(promo_unit_price) as promo_unit_price,
    max(discount_amount) as discount_amount
from(
        select min(time) as time,
            min(time) as min_insert_timestamp,
            min(td_time_parse(shopping_cart_last_modified_date)) as min_update_timestamp,
            max(to_base64url(xxhash64(cast(coalesce(customer_uid, '') || coalesce(shopping_cart_uid, '') || coalesce(sku_code, '') as varbinary)))) as id,
            max(cp_email) as cp_email,
            max(storecode) as storecode,
            max(cart_status) as cart_status,
            max(shopping_cart_uid) as shopping_cart_uid,
            max(sku_code) as sku_code,
            max(currency) as currency,
            max(list_unit_price) as list_unit_price,
            max(quantity) as quantity,
            max(customer_uid) as customer_uid,
            max(item_type) as item_type,
            max(isfreegiftitem) as isfreegiftitem,
            max(cart_item_creation_date) as cart_item_creation_date,
            max(shipping_option_code) as shipping_option_code,
            max(cp_first_name) as cp_first_name,
            max(cp_last_name) as cp_last_name,
            max(cp_phone) as cp_phone,
            SUBSTRING(REGEXP_REPLACE(cp_phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'),1, 20) AS phone_std,
            max(billing_first_name) as billing_first_name,
            max(billing_last_name) as billing_last_name,
            max(billing_street_1) as billing_street_1,
            max(billing_city) as billing_city,
            max(billing_sub_country) as billing_sub_country,
            max(billing_country) as billing_country,
            max(payment_type_name) as payment_type_name,
            max(promo_unit_price) as promo_unit_price,
            max(discount_amount) as discount_amount
        from src_databricks.ptc
        where time > $ { td.last_results.last_session_time }
            and to_base64url(xxhash64(cast( coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary)) ) not in (
                select id
                from rr_tpci_stg.identities
                where id is not null )
            and to_base64url(
                xxhash64(cast(coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary)
                )) is not null
        group by customer_uid,
            shopping_cart_uid,
            sku_code
        union ALL

        select min(time) as time,
            min(time) as min_insert_timestamp,
            min(td_time_parse(shopping_cart_last_modified_date)) as min_update_timestamp,
            max(to_base64url(xxhash64(cast(coalesce(customer_uid, '') || coalesce(shopping_cart_uid, '') || coalesce(sku_code, '') as varbinary)))) as id,
            cast(null as varchar) as   cp_email,
            cast(null as varchar) as  storecode,
            cast(null as varchar) as cart_status,
            cast(null as varchar) as  shopping_cart_uid,
            cast(null as varchar) as  sku_code,
            cast(null as varchar) as  currency,
            cast(null as varchar) as list_unit_price,
            cast(null as varchar) as   quantity,
            cast(null as varchar) as  customer_uid,
            cast(null as varchar) as  item_type,
            cast(null as varchar) as  isfreegiftitem,
            cast(null as varchar) as  cart_item_creation_date,
            cast(null as varchar) as   shipping_option_code,
            cast(null as varchar) as  cp_first_name,
            cast(null as varchar) as cp_last_name,
            cast(null as varchar) as  cp_phone,
            cast(null as varchar) as  phone_std,
            cast(null as varchar) as billing_first_name,
            cast(null as varchar) as  billing_last_name,
            cast(null as varchar) as  billing_street_1,
            cast(null as varchar) as  billing_city,
            cast(null as varchar) as billing_sub_country,
            cast(null as varchar) as  billing_country,
            cast(null as varchar) as  payment_type_name,
            cast(null as varchar) as  promo_unit_price,
            cast(null as varchar) as discount_amount
        from src_databricks.ptc
        where time > $ { td.last_results.last_session_time }
            and to_base64url(xxhash64(cast( coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary)) ) not in (
                select id
                from rr_tpci_stg.identities
                where id is not null )
            and to_base64url(
                xxhash64(cast(coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary)
                )) is not null
        group by customer_uid,
            shopping_cart_uid,
            sku_code
    )