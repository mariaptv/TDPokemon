CREATE TABLE product_inventory AS
SELECT update_date,
    storefront,
    sku,
    quantity
FROM  tpci_stg.sflake_inventory_ats_current_intrmdt_tmp