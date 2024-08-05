DROP TABLE IF EXISTS tpci_stg.sflake_inventory_ats_current_intrmdt_tmp;

CREATE TABLE tpci_stg.sflake_inventory_ats_current_intrmdt_tmp AS
WITH sub_sflake_inventory_ats_current AS (
    SELECT 
        update_date,
        sku,
        time,
        storefront,
        CASE 
            WHEN storefront = 'pokemoncenter.com' THEN 'US' 
            WHEN storefront = 'pokemoncenter.com/en-gb' THEN 'UK' 
            WHEN storefront = 'pokemoncenter.com/en-ca' THEN 'CA' 
            ELSE storefront 
        END AS storefront_2_char_format,
        quantity
    FROM src_snowflake.inventory_ats_current
)
SELECT * FROM sub_sflake_inventory_ats_current;