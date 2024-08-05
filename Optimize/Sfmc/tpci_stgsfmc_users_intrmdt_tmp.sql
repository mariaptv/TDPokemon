DROP TABLE IF EXISTS tpci_stg.sfmc_users_intrmdt_tmp;
CREATE TABLE tpci_stg.sfmc_users_intrmdt_tmp AS WITH sfmc_users_deduped AS (
    SELECT *
    FROM (
            SELECT email,
                opt_in,
                member_id,
                pcenter_email_opt_in,
                last_modified,
                optin_date,
                days_opted_in,
                most_recent_click_date,
                most_recent_click_message_name,
                most_recent_open_date,
                most_recent_open_message_name,
                apple_user,
                subscriber_status,
                total_opens_last_24_months,
                total_clicks_last_24_months,
                unsubscribe_date,
                domain,
                first_name,
                last_name,
                dob,
                ptc_guid,
                total_clicks_last_24_months_1,
                time,
                ROW_NUMBER() OVER (
                    PARTITION BY ptc_guid
                    ORDER BY time DESC
                ) AS rn
            FROM src_sfmc_data.td_users
        )
    WHERE rn = 1
)
SELECT LOWER(opt_in) AS opt_in,
    subscriber_status,
    ROW_NUMBER() OVER ( PARTITION BY email
        ORDER BY time
    ) AS rnk,
    IF(most_recent_open_date = '' OR most_recent_open_date IS NULL, NULL, 
    TD_TIME_PARSE(most_recent_open_date)) AS most_recent_open_date_unixtime,
    ptc_guid,
    dob,
    member_id,
    IF(last_name IS NULL OR last_name = 'NULL', '', last_name) AS last_name,
    IF(pcenter_email_opt_in = '', NULL, LOWER(pcenter_email_opt_in)) AS pcenter_email_opt_in,
    first_name || ' ' || last_name AS name,
    most_recent_open_message_name,
    IF(first_name IS NULL OR first_name = 'NULL', '', first_name) AS first_name,
    domain,
    TRIM(LOWER(domain)) AS email_domain,
    optin_date,
    IF(total_clicks_last_24_months = '', NULL, CAST(total_clicks_last_24_months AS INT)) AS total_clicks_last_24_months,
    SUBSTRING(last_modified, 1, 19) AS last_modified,
    days_opted_in,
    total_clicks_last_24_months_1,
    CASE
        WHEN domain IN (
            SELECT exception_value
            FROM src_snowflake.exception_list_email
        ) THEN NULL
        WHEN email LIKE '%@%' THEN CONCAT(
            LOWER(TRIM(email)),
            ':',
            COALESCE(first_name, '')
        )
        ELSE NULL
    END AS email_std_fname,
    apple_user,
    CASE
        WHEN SPLIT_PART(LOWER(email), '@', 2) IN (
            SELECT exception_value
            FROM src_snowflake.exception_list
        ) THEN NULL
        WHEN email LIKE '%@%' THEN LOWER(TRIM(email))
        ELSE NULL
    END AS email_std,
    IF(optin_date = '' OR optin_date IS NULL, NULL, TD_TIME_PARSE(optin_date)) AS optin_date_unixtime,
    SUBSTRING(most_recent_open_date, 1, 19) AS most_recent_open_date,
    TD_TIME_PARSE(SUBSTRING(most_recent_click_date, 1, 19)) AS most_recent_click_date_unixtime,
    most_recent_click_message_name,
    IF(unsubscribe_date = '' OR unsubscribe_date IS NULL, NULL, TD_TIME_PARSE(unsubscribe_date)) AS unsubscribe_date_unixtime,
    unsubscribe_date,
    IF(total_opens_last_24_months = '', NULL, CAST(total_opens_last_24_months AS INT)) AS total_opens_last_24_months,
    IF(last_modified = '' OR last_modified IS NULL, NULL, TD_TIME_PARSE(last_modified)) AS last_modified_unixtime,
    IF(subscriber_status = 'active', 'true', 'false') AS subscriber_flag,
    email,
    SUBSTRING(most_recent_click_date, 1, 19) AS most_recent_click_date
FROM sfmc_users_deduped
