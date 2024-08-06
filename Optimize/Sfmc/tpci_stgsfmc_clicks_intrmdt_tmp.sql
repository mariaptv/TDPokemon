DROP TABLE IF EXISTS tpci_stg.sfmc_clicks_intrmdt_tmp;

CREATE TABLE tpci_stg.sfmc_clicks_intrmdt_tmp AS
WITH sub_sfmc_clicks AS (
    SELECT 
        time,
        email_send_date,
        domain,
        link_name,
        TD_TIME_PARSE(email_send_date) AS email_send_date_unixtime,
        SUBSTRING(event_date, 1, 19) AS event_date,
        email,
        CASE 
            WHEN SPLIT_PART(LOWER(email), '@', 2) IN (SELECT exception_value FROM src_snowflake.exception_list) 
            THEN NULL 
            WHEN email LIKE '%@%' THEN LOWER(TRIM(email)) 
            ELSE NULL 
        END AS email_std,
        is_unique,
        url,
        link_content,
        REPLACE(from_name, '�', 'e') AS from_name,
        TD_TIME_PARSE(event_date) AS event_date_unixtime,
        campaign_names,
        job_id,
        subscriber_key,
        email_name
    FROM src_sfmc_data.td_clicks
)
SELECT * FROM sub_sfmc_clicks;