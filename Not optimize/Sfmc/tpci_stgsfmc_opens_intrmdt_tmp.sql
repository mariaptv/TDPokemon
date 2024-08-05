-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=9=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sfmc_opens_intrmdt_tmp;
CREATE TABLE tpci_stg.sfmc_opens_intrmdt_tmp AS WITH sfmc_opens_1 AS (
    SELECT *
    FROM src_sfmc_data.td_opens
),
sub_sfmc_opens AS (
    SELECT email_send_date AS email_send_date,
        time AS time,
        domain AS domain,
        subscriber_key AS subscriber_key,
        TD_TIME_PARSE(event_date) AS event_date_unixtime,
        email_name AS email_name,
        is_unique AS is_unique,
        campaign_names AS campaign_names,
        event_date AS event_date,
        TD_TIME_PARSE(email_send_date) AS email_send_date_unixtime,
        email AS email,
        job_id AS job_id,
        replace(from_name, '�', 'e') AS from_name,
        CASE
            WHEN split_part(lower(email), '@', 2) in (
                select exception_value
                from src_snowflake.exception_list
            ) then null
            WHEN email like '%@%' THEN lower(trim(email))
            else null
        end AS email_std
    FROM sfmc_opens_1
)
select *
from sub_sfmc_opens