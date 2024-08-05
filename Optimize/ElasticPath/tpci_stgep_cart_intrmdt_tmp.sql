DROP TABLE IF EXISTS tpci_stg.ep_cart_intrmdt_tmp;

CREATE TABLE tpci_stg.ep_cart_intrmdt_tmp AS
WITH sub_ep_cart AS (
    SELECT
        quantity,
        billing_sub_country,
        shipping_street_1,
        SUBSTRING(REGEXP_REPLACE(cp_phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'), 1, 20) AS phone_std,
        shipping_last_name,
        promo_unit_price,
        cp_phone,
        shipping_country,
        customer_uid,
        billing_first_name,
        shipping_city,
        TD_TIME_PARSE(cart_item_last_modified_date) AS cart_item_last_modified_date_unixtime,
        billing_zip_postal_code,
        discount_amount,
        cart_item_creation_date,
        isfreegiftitem,
        cart_status,
        TD_TIME_PARSE(cart_item_creation_date) AS cart_item_creation_date_unixtime,
        currency,
        billing_country,
        cp_first_name,
        storecode,
        sku_code,
        cp_first_name ||' ' || cp_last_name AS name,
        TD_TIME_PARSE(shopping_cart_last_modified_date) AS shopping_cart_last_modified_date_unixtime,
        shopping_cart_last_modified_date,
        cart_item_last_modified_date,
        billing_street_2,
        CASE 
            WHEN SPLIT_PART(LOWER(cp_email), '@', 2) IN (SELECT exception_value FROM src_snowflake.exception_list) THEN NULL 
            ELSE LOWER(TRIM(cp_email))
        END AS email_std,
        shopping_cart_uid,
        shipping_sub_country,
        shipping_first_name,
        billing_city,
        list_unit_price,
        item_type,
        shipping_street_2,
        cp_email,
        sale_unit_price,
        shipping_zip_postal_code,
        rule_display_name,
        time,
        shipping_option_code,
        billing_street_1,
        cp_last_name,
        billing_last_name,
        payment_type_name,
        parent_item_uid
    FROM src_elastic_path_data.cart
)
SELECT * FROM sub_ep_cart;

