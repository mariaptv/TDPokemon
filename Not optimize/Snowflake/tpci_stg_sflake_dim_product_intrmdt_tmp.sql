-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=3=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sflake_dim_product_intrmdt_tmp;
CREATE TABLE tpci_stg.sflake_dim_product_intrmdt_tmp AS WITH sflake_dim_product_1 AS (
    SELECT *
    FROM src_snowflake.dim_product
),
sub_sflake_dim_product AS (
    SELECT quantity AS quantity,
        trailing_week_daily_average_sold AS trailing_week_daily_average_sold,
        hot_product AS hot_product,
        trailing_week_total_sold AS trailing_week_total_sold,
        latest_item_rec_id AS latest_item_rec_id,
        available_to_purchase_date AS available_to_purchase_date,
        sku AS sku,
        CASE
            WHEN (2 * trailing_week_daily_average_sold) > (quantity - 3 * trailing_week_daily_average_sold)
            AND (4 * trailing_week_daily_average_sold) < (
                quantity - 3 * trailing_week_daily_average_sold * 3
            ) THEN 'True'
            else 'False'
        end AS three_day_low_inventory_prediction_flag,
        authorized_to_sell AS authorized_to_sell,
        time AS time,
        prf AS prf,
        item_number AS item_number,
        product_url AS product_url,
        product_key AS product_key,
        sku_description AS sku_description,
        theme AS theme,
        sku_web_description AS sku_web_description,
        style_description AS style_description,
        subcategory AS subcategory,
        is_displayable AS is_displayable,
        class AS class,
        is_buyable AS is_buyable,
        CASE
            WHEN storefront = 'pokemoncenter.com' THEN 'US'
            WHEN storefront = 'pokemoncenter.com/en-gb' THEN 'UK'
            WHEN storefront = 'pokemoncenter.com/en-ca' THEN 'CA'
            ELSE storefront
        END AS storefront_2_char_format,
        CASE
            WHEN cast(preorder_end_date as timestamp) > CURRENT_TIMESTAMP THEN 'True'
            else 'False'
        end AS preorder_flag,
        logistics_variant_id AS logistics_variant_id,
        raelease_date AS raelease_date,
        storefront AS storefront,
        preorder_end_date AS preorder_end_date,
        category AS category,
        product_image_url AS product_image_url,
        character_list AS character_list,
        original_item_rec_id AS original_item_rec_id,
        preorder_start_date AS preorder_start_date
    FROM sflake_dim_product_1
)
select *
from sub_sflake_dim_product