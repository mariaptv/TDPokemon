-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=6=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sfmc_users_intrmdt_tmp;
CREATE TABLE tpci_stg.sfmc_users_intrmdt_tmp AS WITH sfmc_users_1 AS (
    SELECT *
    FROM src_sfmc_data.td_users
),
sfmc_users_2 AS (
    SELECT email AS email,
        opt_in AS opt_in,
        member_id AS member_id,
        pcenter_email_opt_in AS pcenter_email_opt_in,
        last_modified AS last_modified,
        optin_date AS optin_date,
        days_opted_in AS days_opted_in,
        most_recent_click_date AS most_recent_click_date,
        most_recent_click_message_name AS most_recent_click_message_name,
        most_recent_open_date AS most_recent_open_date,
        most_recent_open_message_name AS most_recent_open_message_name,
        apple_user AS apple_user,
        subscriber_status AS subscriber_status,
        total_opens_last_24_months AS total_opens_last_24_months,
        total_clicks_last_24_months AS total_clicks_last_24_months,
        unsubscribe_date AS unsubscribe_date,
        domain AS domain,
        first_name AS first_name,
        last_name AS last_name,
        dob AS dob,
        ptc_guid AS ptc_guid,
        total_clicks_last_24_months_1 AS total_clicks_last_24_months_1,
        time AS time
    FROM (
            SELECT email AS email,
                opt_in AS opt_in,
                member_id AS member_id,
                pcenter_email_opt_in AS pcenter_email_opt_in,
                last_modified AS last_modified,
                optin_date AS optin_date,
                days_opted_in AS days_opted_in,
                most_recent_click_date AS most_recent_click_date,
                most_recent_click_message_name AS most_recent_click_message_name,
                most_recent_open_date AS most_recent_open_date,
                most_recent_open_message_name AS most_recent_open_message_name,
                apple_user AS apple_user,
                subscriber_status AS subscriber_status,
                total_opens_last_24_months AS total_opens_last_24_months,
                total_clicks_last_24_months AS total_clicks_last_24_months,
                unsubscribe_date AS unsubscribe_date,
                domain AS domain,
                first_name AS first_name,
                last_name AS last_name,
                dob AS dob,
                ptc_guid AS ptc_guid,
                total_clicks_last_24_months_1 AS total_clicks_last_24_months_1,
                time AS time,
                ROW_NUMBER() OVER (
                    PARTITION BY ptc_guid
                    ORDER BY time desc
                ) rn
            FROM sfmc_users_1
        ) a
    WHERE rn = 1
),
sub_sfmc_users AS (
    SELECT lower(opt_in) AS opt_in,
        subscriber_status AS subscriber_status,
        row_number() over (
            partition by email
            order by time
        ) AS rnk,
        case
            when most_recent_open_date = ''
            or most_recent_open_date is null then null
            else TD_TIME_PARSE(most_recent_open_date)
        end AS most_recent_open_date_unixtime,
        ptc_guid AS ptc_guid,
        dob AS dob,
        member_id AS member_id,
        case
            when last_name is null
            or last_name = 'NULL' THEN ''
            ELSE last_name
        end AS last_name,
        case
            when pcenter_email_opt_in = '' then null
            else lower(pcenter_email_opt_in)
        end AS pcenter_email_opt_in,
        time AS time,
        concat(first_name, ' ', last_name) AS name,
        most_recent_open_message_name AS most_recent_open_message_name,
        case
            when first_name is null
            or first_name = 'NULL' THEN ''
            ELSE first_name
        end AS first_name,
        domain AS domain,
        trim(lower(domain)) AS email_domain,
        optin_date AS optin_date,
        CASE
            WHEN total_clicks_last_24_months = '' THEN null
            else cast(total_clicks_last_24_months as int)
        end AS total_clicks_last_24_months,
        SUBSTRING(last_modified, 1, 19) AS last_modified,
        days_opted_in AS days_opted_in,
        total_clicks_last_24_months_1 AS total_clicks_last_24_months_1,
        CASE
            WHEN domain in (
                select exception_value
                from src_snowflake.exception_list_email
            ) then null
            WHEN email like '%@%' THEN concat(lower(trim(email)), ':', coalesce(first_name, ''))
            else null
        end AS email_std_fname,
        apple_user AS apple_user,
        CASE
            WHEN split_part(lower(email), '@', 2) in (
                select exception_value
                from src_snowflake.exception_list
            ) then null
            WHEN email like '%@%' THEN lower(trim(email))
            else null
        end AS email_std,
        case
            when optin_date = ''
            or optin_date is null then null
            else TD_TIME_PARSE(optin_date)
        end AS optin_date_unixtime,
        SUBSTRING(most_recent_open_date, 1, 19) AS most_recent_open_date,
        TD_TIME_PARSE(SUBSTRING(most_recent_click_date, 1, 19)) AS most_recent_click_date_unixtime,
        most_recent_click_message_name AS most_recent_click_message_name,
        case
            when unsubscribe_date = ''
            or unsubscribe_date is null then null
            else TD_TIME_PARSE(unsubscribe_date)
        end AS unsubscribe_date_unixtime,
        unsubscribe_date AS unsubscribe_date,
        CASE
            WHEN total_opens_last_24_months = '' THEN null
            else cast(total_opens_last_24_months as int)
        end AS total_opens_last_24_months,
        case
            when last_modified = ''
            or last_modified is null then null
            else TD_TIME_PARSE(last_modified)
        end AS last_modified_unixtime,
        case
            when subscriber_status = 'active' then 'true'
            else 'false'
        end AS subscriber_flag,
        email AS email,
        SUBSTRING(most_recent_click_date, 1, 19) AS most_recent_click_date
    FROM sfmc_users_2
)
select *
from sub_sfmc_users