DROP TABLE IF EXISTS tpci_stg.sfmc_opens_intrmdt_tmp;

CREATE TABLE tpci_stg.sfmc_opens_intrmdt_tmp AS
WITH sub_sfmc_opens AS (
    SELECT 
        email_send_date,
        time,
        domain,
        subscriber_key,
        TD_TIME_PARSE(event_date) AS event_date_unixtime,
        email_name,
        is_unique,
        campaign_names,
        event_date,
        TD_TIME_PARSE(email_send_date) AS email_send_date_unixtime,
        email,
        job_id,
        REPLACE(from_name, '�', 'e') AS from_name,
        CASE 
            WHEN SPLIT_PART(LOWER(email), '@', 2) IN (SELECT exception_value FROM src_snowflake.exception_list) THEN NULL 
            WHEN email LIKE '%@%' THEN LOWER(TRIM(email)) 
            ELSE NULL 
        END AS email_std
    FROM src_sfmc_data.td_opens
)
SELECT * FROM sub_sfmc_opens;