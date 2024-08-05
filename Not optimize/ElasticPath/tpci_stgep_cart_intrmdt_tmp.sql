-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=1=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.ep_cart_intrmdt_tmp;
CREATE TABLE tpci_stg.ep_cart_intrmdt_tmp AS WITH ep_cart_1 AS (
    SELECT *
    FROM src_elastic_path_data.cart
),
sub_ep_cart AS (
    SELECT quantity AS quantity,
        billing_sub_country AS billing_sub_country,
        shipping_street_1 AS shipping_street_1,
        CASE
            WHEN cp_phone like '+1%' THEN trim(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(REPLACE(REPLACE(cp_phone, '+1'), '-'), '.'),
                                    '#'
                                ),
                                ','
                            ),
                            ')'
                        ),
                        '('
                    ),
                    ' '
                )
            )
            WHEN cp_phone like '1%' THEN trim(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(REPLACE(SUBSTRING(cp_phone, 2, 20), '-'), '.'),
                                    '#'
                                ),
                                ','
                            ),
                            ')'
                        ),
                        '('
                    ),
                    ' '
                )
            )
            ELSE trim(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(REPLACE(REPLACE(cp_phone, '+'), '-'), '.'),
                                    '#'
                                ),
                                ','
                            ),
                            ')'
                        ),
                        '('
                    ),
                    ' '
                )
            )
        END AS phone_std,
        shipping_last_name AS shipping_last_name,
        promo_unit_price AS promo_unit_price,
        cp_phone AS cp_phone,
        shipping_country AS shipping_country,
        customer_uid AS customer_uid,
        billing_first_name AS billing_first_name,
        shipping_city AS shipping_city,
        TD_TIME_PARSE(cart_item_last_modified_date) AS cart_item_last_modified_date_unixtime,
        billing_zip_postal_code AS billing_zip_postal_code,
        discount_amount AS discount_amount,
        cart_item_creation_date AS cart_item_creation_date,
        isfreegiftitem AS isfreegiftitem,
        cart_status AS cart_status,
        TD_TIME_PARSE(cart_item_creation_date) AS cart_item_creation_date_unixtime,
        currency AS currency,
        billing_country AS billing_country,
        cp_first_name AS cp_first_name,
        storecode AS storecode,
        sku_code AS sku_code,
        concat(cp_first_name, ' ', cp_last_name) AS name,
        TD_TIME_PARSE(shopping_cart_last_modified_date) AS shopping_cart_last_modified_date_unixtime,
        shopping_cart_last_modified_date AS shopping_cart_last_modified_date,
        cart_item_last_modified_date AS cart_item_last_modified_date,
        billing_street_2 AS billing_street_2,
        CASE
            WHEN split_part(lower(cp_email), '@', 2) in (
                select exception_value
                from src_snowflake.exception_list
            ) then null
            else lower(trim(cp_email))
        end AS email_std,
        shopping_cart_uid AS shopping_cart_uid,
        shipping_sub_country AS shipping_sub_country,
        shipping_first_name AS shipping_first_name,
        billing_city AS billing_city,
        list_unit_price AS list_unit_price,
        item_type AS item_type,
        shipping_street_2 AS shipping_street_2,
        cp_email AS cp_email,
        sale_unit_price AS sale_unit_price,
        shipping_zip_postal_code AS shipping_zip_postal_code,
        rule_display_name AS rule_display_name,
        time AS time,
        shipping_option_code AS shipping_option_code,
        billing_street_1 AS billing_street_1,
        cp_last_name AS cp_last_name,
        billing_last_name AS billing_last_name,
        payment_type_name AS payment_type_name,
        parent_item_uid AS parent_item_uid
    FROM ep_cart_1
)
select *
from sub_ep_cart