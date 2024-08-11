-- Create the table
CREATE TABLE if not exists rr_tpci_stg.campaign_events (
  time bigint,  
  email VARCHAR,  
  event_name VARCHAR,  
  subscriber_key VARCHAR,  
  link_name VARCHAR,
  link_content VARCHAR,  
  email_name VARCHAR,  
  domain VARCHAR, 
  message_id VARCHAR,  
  message_content VARCHAR,  
  platform VARCHAR,  
  template VARCHAR, 
  status VARCHAR 
)
with (
  bucketed_on = array['id'],
  bucket_count = 512
)

INSERT INTO rr_tpci_stg.campaign_events 
SELECT min(time) as time,
min(time) as min_insert_timestamp, 
min(min_update_timestamp) as min_update_timestamp,
id as id,
email as email,
event_name as event_name,
subscriber_key as subscriber_key,
link_name as link_name,
link_content as link_content,
email_name as email_name,
domain as domain,
message_id as message_id,
message_content AS message_content,
platform as platform,
template as template,
status as status 
FROM (
SELECT 
  min(event_date) as time,
  min(event_date) as min_insert_timestamp,
  min(td_time_parse(event_date)) as min_update_timestamp,
  max(to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')as varbinary)))) as id,
  min(email) as email,
  'clicks' as event_name,
  CAST(subscriber_key AS VARCHAR) as subscriber_key,
  min(CAST(TD_TIME_PARSE(email_send_date) as VARCHAR)) AS email_send_date_unixtime,
  max(link_name) as link_name,
  max(link_content) as link_content,
  max(email_name) as email_name,
  cast(null as varchar) as domain,
  cast(null as varchar) as message_id,
  cast(null as varchar) as message_content,
  cast(null as varchar) as platform,
  cast(null as varchar) as template,
  cast(null as varchar) as status,
  cast(null as varchar) as request_id
FROM src_sfmc_data.td_clicks 
where
    time > ${td.last_results.last_session_time} and 
    to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')))) not in (select id from rr_tpci_stg.campaign_events where id is not null)
    and to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')))) is not null
group by email, subscriber_key

UNION ALL
SELECT 
 min(event_date) as time,
  min(event_date) as min_insert_timestamp,
  min(td_time_parse(event_date)) as min_update_timestamp,
  max(to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(CAST(subscriber_key AS VARCHAR),'')as varbinary)))) as id,
  min(email) as email,
  'opens' as event_name,
  CAST(subscriber_key AS VARCHAR) as subscriber_key,
  min(CAST(TD_TIME_PARSE(email_send_date)as VARCHAR)) AS email_send_date_unixtime,
  cast(null as varchar) as link_name,
  cast(null as varchar) as link_content,
  cast(null as varchar) as email_name,
  max(domain) as domain,
  cast(null as varchar) as message_id,
  cast(null as varchar) as message_content,
  cast(null as varchar) as platform,
  cast(null as varchar) as template,
  cast(null as varchar) as status,
  cast(null as varchar) as  request_id
FROM src_sfmc_data.td_opens opens
where
    time > ${td.last_results.last_session_time} and 
    to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')as varbinary))) not in (select id from rr_tpci_stg.campaign_events where id is not null)
    and to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')))) is not null
group by email, subscriber_key

UNION ALL
SELECT 
  min(date_time_send) as time,
  min(date_time_send) as min_insert_timestamp,
  min(td_time_parse(date_time_send)) as min_update_timestamp,
  max(to_base64url(xxhash64(cast(coalesce(CAST(message_id AS VARCHAR),'') || coalesce(request_id,'')as varbinary)))) as id,
  cast(null as varchar) as email,
  'mobile' as event_name,
  cast(null as varchar) as subscriber_key,
  cast(null as varchar) AS email_send_date_unixtime,
  cast(null as varchar) as link_name,
  cast(null as varchar) as link_content,
  cast(null as varchar) as email_name,
  cast(null as varchar) as dommain,
  max(message_id) as message_id,
  max(message_content) as message_content,
  max(platform) as platform,
  max(template) as template,
  max(status) as status,
  max(request_id) as request_id
FROM src_sfmc_data.mobile_data 
where
    time > ${td.last_results.last_session_time} and 
    to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')as varbinary))) not in (select id from rr_tpci_stg.campaign_events where id is not null)
    and to_base64url(xxhash64(cast(coalesce(email,'') || coalesce(subscriber_key,'')))) is not null
group by message_id, request_id)