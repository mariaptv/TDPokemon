-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=8=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sfmc_clicks_intrmdt_tmp; CREATE TABLE tpci_stg.sfmc_clicks_intrmdt_tmp AS WITH 
sfmc_clicks_1 AS  (SELECT
  *
FROM
src_sfmc_data.td_clicks),

sub_sfmc_clicks AS  (SELECT time  AS  time,
email_send_date  AS  email_send_date,
domain  AS  domain,
link_name  AS  link_name,
TD_TIME_PARSE(email_send_date)   AS  email_send_date_unixtime,
SUBSTRING(event_date,1,19)   AS  event_date,
email  AS  email,
CASE WHEN split_part(lower(email),'@',2) in (select exception_value from src_snowflake.exception_list) then null WHEN email like '%@%' THEN lower(trim(email)) else null  end    AS  email_std,
is_unique  AS  is_unique,
url  AS  url,
link_content  AS  link_content,
replace(from_name,'�','e')     AS  from_name,
TD_TIME_PARSE(event_date)   AS  event_date_unixtime,
campaign_names  AS  campaign_names,
job_id  AS  job_id,
subscriber_key  AS  subscriber_key,
email_name  AS  email_name 
FROM
  sfmc_clicks_1)

select * from sub_sfmc_clicks