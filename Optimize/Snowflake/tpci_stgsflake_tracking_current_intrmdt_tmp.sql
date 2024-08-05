INSERT INTO tpci_stg.sflake_tracking_current_intrmdt_tmp
SELECT 
    event_state,
    package_carrier_moniker,
    event_date,
    fulfillment_id,
    dest_city,
    time,
    status_code_description,
    service_description,
    dest_state,
    tracking_number,
    TRIM(dest_country) AS dest_country,
    event_country,
    dest_zipcode,
    event_city
FROM
    src_snowflake.tracking_current;