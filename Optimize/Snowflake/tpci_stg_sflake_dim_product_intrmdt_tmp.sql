DROP TABLE IF EXISTS tpci_stg.sflake_dim_product_intrmdt_tmp;
CREATE TABLE tpci_stg.sflake_dim_product_intrmdt_tmp AS WITH sub_sflake_dim_product AS (
    SELECT quantity,
        trailing_week_daily_average_sold,
        hot_product,
        trailing_week_total_sold,
        latest_item_rec_id,
        available_to_purchase_date,
        sku,
        IF(
            (2 * trailing_week_daily_average_sold) > (quantity - 3 * trailing_week_daily_average_sold)
            AND (4 * trailing_week_daily_average_sold) < (
                quantity - 3 * trailing_week_daily_average_sold * 3
            ),
            'True',
            'False'
        ) AS three_day_low_inventory_prediction_flag,
        authorized_to_sell,
        time,
        prf,
        item_number,
        product_url,
        product_key,
        sku_description,
        theme,
        sku_web_description,
        style_description,
        subcategory,
        is_displayable,
        class,
        is_buyable,
        CASE
            WHEN storefront = 'pokemoncenter.com' THEN 'US'
            WHEN storefront = 'pokemoncenter.com/en-gb' THEN 'UK'
            WHEN storefront = 'pokemoncenter.com/en-ca' THEN 'CA'
            ELSE storefront
        END AS storefront_2_char_format,
        IF(
            CAST(preorder_end_date AS timestamp) > CURRENT_TIMESTAMP,
            'True',
            'False'
        ) AS preorder_flag,
        logistics_variant_id,
        raelease_date,
        storefront,
        preorder_end_date,
        category,
        product_image_url,
        character_list,
        original_item_rec_id,
        preorder_start_date
    FROM src_snowflake.dim_product
)
SELECT *
FROM sub_sflake_dim_product;