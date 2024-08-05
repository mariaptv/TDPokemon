-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=10=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sfmc_mobile_intrmdt_tmp; CREATE TABLE tpci_stg.sfmc_mobile_intrmdt_tmp AS WITH 
sfmc_mobile_1 AS  (SELECT
  *
FROM
src_sfmc_data.mobile_data),

sub_sfmc_mobile AS  (SELECT platform  AS  platform,
request_id  AS  request_id,
ios_media_url  AS  ios_media_url,
open_date  AS  open_date,
push_job_id  AS  push_job_id,
inbox_message_downloaded  AS  inbox_message_downloaded,
service_response  AS  service_response,
message_id  AS  message_id,
message_name  AS  message_name,
android_media_url  AS  android_media_url,
contact_key  AS  contact_key,
media_alt  AS  media_alt,
status  AS  status,
TD_TIME_PARSE(open_date)   AS  open_date_unixtime,
campaigns  AS  campaigns,
time_in_app  AS  time_in_app,
template  AS  template,
inbox_message_opened  AS  inbox_message_opened,
date_time_send  AS  date_time_send,
page_name  AS  page_name,
campaign_names  AS  campaign_names,
message_opened  AS  message_opened,
time  AS  time,
TD_TIME_PARSE(date_time_send)   AS  date_time_send_unixtime,
format  AS  format,
platform_version  AS  platform_version,
device_id  AS  device_id,
trim(app_name)   AS  app_name,
geofence_name  AS  geofence_name,
message_content  AS  message_content 
FROM
  sfmc_mobile_1)

select * from sub_sfmc_mobile