-- Create the table
CREATE TABLE campaign_events (
  time TIME,  -- Assuming event_date_unixtime is a unix timestamp
  email VARCHAR(255),  -- Adjust length if needed
  event_name VARCHAR(255),  
  subscriber_key VARCHAR(255),  
  link_name VARCHAR(255),  
  link_content VARCHAR(255),  
  email_name VARCHAR(255),  
  domain VARCHAR(255) ,  -- Allows null values
  message_id VARCHAR(255),  
  message_content VARCHAR(255),  
  platform VARCHAR(255) ,  
  template VARCHAR(255) , 
  status VARCHAR(255) 
);


CREATE TABLE campaign_events AS
SELECT 
  clicks.event_date_unixtime as time,
  clicks.email as email,
  'clicks' as event_name,
  clicks.subscriber_key as subscriber_key,
  clicks.link_name as link_name,
  clicks.link_content as link_content,
  clicks.email_name as email_name,
  NULL as domain,
  NULL AS message_id,
  NULL AS message_content,
  NULL as platform,
  NULL as template,
  NULL as status
FROM tpci_stg.sfmc_clicks_intrmdt_tmp clicks
UNION ALL
SELECT 
  opens.email_send_date_unixtime as time,
  opens.email as email,
  'opens' as event_name,
  opens.subscriber_key as subscriber_key,
  NULL AS link_name,
  NULL AS link_content,
  NULL AS email_name,
  opens.domain as domain,
  NULL AS message_id,
  NULL AS message_content,
  NULL as platform,
  NULL as template,
  NULL as status
FROM tpci_stg.sfmc_opens_intrmdt_tmp opens
UNION ALL
SELECT 
  mobile.date_time_send_unixtime as time,
  NULL as email,
  'mobile' as event_name,
  NULL as subscriber_key,
  NULL AS link_name,
  NULL AS link_content,
  NULL AS email_name,
  NULL as dommain,
  mobile.message_id as message_id,
  mobile.message_content as message_content,
  mobile.platform as platform,
  mobile.template as template,
  mobile.status as status
FROM tpci_stg.sfmc_mobile_intrmdt_tmp mobile;