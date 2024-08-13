CREATE TABLE if not exists rr_tpci_stg.campaign_events (
  event_time bigint,
  event_time_unixtime bigint,
  id VARCHAR,
  email_address VARCHAR,
  event_name VARCHAR,
  subscriber_key VARCHAR,
  email_send_date_unixtime VARCHAR,
  link_name VARCHAR,
  link_content VARCHAR,
  email_name VARCHAR,
  from_name VARCHAR,
  domain VARCHAR,
  campaign_names VARCHAR,
  job_id VARCHAR,
  message_id VARCHAR,
  message_content VARCHAR,
  platform VARCHAR,
  template VARCHAR,
  status VARCHAR,
  request_id VARCHAR,
  app_name VARCHAR,
  message_name VARCHAR,
  message_opened VARCHAR,
  service_response VARCHAR,
  template VARCHAR,
  contact_key VARCHAR,
  device_id VARCHAR
) with (
  bucketed_on = array ['id'],
  bucket_count = 512
)
INSERT INTO rr_tpci_stg.campaign_events
SELECT event_time as event_time,
  event_time_unixtime as event_time_unixtime,
  NULLIF(TRIM(id), '') as id,
  NULLIF(TRIM(email_address), '') as email_addres,
  NULLIF(TRIM(event_name), '') as event_name,
  NULLIF(TRIM(subscriber_key), '') as subscriber_key,
  NULLIF(TRIM(email_send_date_unixtime), '') AS email_send_date_unixtime,
  NULLIF(TRIM(link_name), '') as link_name,
  NULLIF(TRIM(link_content), '') as link_content,
  NULLIF(TRIM(email_name), '') as email_name,
  NULLIF(TRIM(from_name), '') AS from_name,
  NULLIF(TRIM(domain), '') as domain,
  NULLIF(TRIM(campaign_names), '') AS campaign_names,
  NULLIF(TRIM(job_id), '') AS job_id,
  NULLIF(TRIM(message_id), '') as message_id,
  NULLIF(TRIM(message_content), '') AS message_content,
  NULLIF(TRIM(platform), '') as platform,
  NULLIF(TRIM(template), '') as template,
  NULLIF(TRIM(status), '') as status,
  NULLIF(TRIM(request_id), '') AS request_id
FROM (
    SELECT event_date AS event_time,
      td_time_parse(event_date) AS event_time_unixtime,
      to_base64url(
        xxhash64(
          cast(
            coalesce(email, '') || coalesce(subscriber_key, '') AS varbinary
          )
        )
      ) AS id,
      CASE
        WHEN REGEXP_EXTRACT(LOWER(TRIM(email)), '.+@(.+)', 1) IN (
          SELECT exception_value
          FROM src_snowflake.exception_list
        ) THEN NULL
        WHEN REGEXP_LIKE(email, '.+@.+') THEN LOWER(TRIM(email))
        ELSE NULL
      END AS email_address,
      'clicks' AS event_name,
      CAST(subscriber_key AS VARCHAR) AS subscriber_key,
      CAST(TD_TIME_PARSE(email_send_date) AS VARCHAR) AS email_send_date_unixtime,
      link_name,
      link_content,
      email_name,
      REPLACE(from_name, '�', 'e') AS from_name,
      domain AS domain,
      campaign_names AS campaign_names,
      CAST(job_id AS VARCHAR) AS job_id,
      CAST(NULL AS VARCHAR) AS message_id,
      CAST(NULL AS VARCHAR) AS message_content,
      CAST(NULL AS VARCHAR) AS platform,
      CAST(NULL AS VARCHAR) AS status,
      CAST(NULL AS VARCHAR) AS request_id,
      CAST(NULL AS VARCHAR) AS app_name,
      CAST(NULL AS VARCHAR) AS message_name,
      CAST(NULL AS VARCHAR) AS message_opened,
      CAST(NULL AS VARCHAR) AS service_response,
      CAST(NULL AS VARCHAR) AS template,
      CAST(NULL AS VARCHAR) AS contact_key,
      CAST(NULL AS VARCHAR) AS device_id
    FROM src_sfmc_data.td_clicks
    where time > $ { td.last_results.last_session_time }
      and to_base64url(
        xxhash64(
          cast(
            coalesce(email, '') || coalesce(subscriber_key, '')
          )
        )
      ) not in (
        select id
        from rr_tpci_stg.campaign_events
        where id is not null
      )
      and to_base64url(
        xxhash64(
          cast(
            coalesce(email, '') || coalesce(subscriber_key, '')
          )
        )
      ) is not null
    UNION ALL
    SELECT event_date AS event_time,
      td_time_parse(event_date) AS min_update_timestamp,
      to_base64url(
        xxhash64(
          cast(
            coalesce(email, '') || coalesce(CAST(subscriber_key AS VARCHAR), '') as varbinary
          )
        )
      ) AS id,
      CASE
        WHEN REGEXP_EXTRACT(LOWER(TRIM(email)), '.+@(.+)', 1) IN (
          SELECT exception_value
          FROM src_snowflake.exception_list
        ) THEN NULL
        WHEN REGEXP_LIKE(email, '.+@.+') THEN LOWER(TRIM(email))
        ELSE NULL
      END AS email_address,
      'opens' AS event_name,
      CAST(subscriber_key AS VARCHAR) AS subscriber_key,
      CAST(TD_TIME_PARSE(email_send_date) AS VARCHAR) AS email_send_date_unixtime,
      cast(null as varchar) AS link_name,
      cast(null as varchar) AS link_content,
      email_name,
      REPLACE(from_name, '�', 'e') AS from_name,
      domain AS domain,
      campaign_names,
      CAST(job_id AS VARCHAR) AS job_id,
      CAST(NULL AS VARCHAR) AS message_id,
      CAST(NULL AS VARCHAR) AS message_content,
      CAST(NULL AS VARCHAR) AS platform,
      CAST(NULL AS VARCHAR) AS template,
      CAST(NULL AS VARCHAR) AS status,
      CAST(NULL AS VARCHAR) AS request_id,
      CAST(NULL AS VARCHAR) AS app_name,
      CAST(NULL AS VARCHAR) AS message_name,
      CAST(NULL AS VARCHAR) AS message_opened,
      CAST(NULL AS VARCHAR) AS service_response,
      CAST(NULL AS VARCHAR) AS contact_key,
      CAST(NULL AS VARCHAR) AS device_id
    FROM src_sfmc_data.td_opens
    where time > $ { td.last_results.last_session_time }
      and to_base64url(
        xxhash64(
          cast(
            coalesce(email, '') || coalesce(subscriber_key, '') as varbinary
          )
        )
      ) not in (
        select id
        from rr_tpci_stg.campaign_events
        where id is not null
      )
      and to_base64url(
        xxhash64(
          cast(
            coalesce(email, '') || coalesce(subscriber_key, '')
          )
        )
      ) is not null
    UNION ALL
    SELECT date_time_send AS event_time,
      td_time_parse(date_time_send) AS event_time_unixtime,
      to_base64url(
        xxhash64(
          cast(
            coalesce(CAST(message_id AS VARCHAR), '') || coalesce(request_id, '') as varbinary
          )
        )
      ) as id,
      cast(null as varchar) as email_address,
      'mobile' as event_name,
      CAST(NULL AS VARCHAR) AS subscriber_key,
      CAST(TD_TIME_PARSE(date_time_send) AS VARCHAR) AS email_send_date_unixtime,
      CAST(NULL AS VARCHAR) AS link_name,
      CAST(NULL AS VARCHAR) AS link_content,
      CAST(NULL AS VARCHAR) AS email_name,
      CAST(NULL AS VARCHAR) AS from_name,
      CAST(NULL AS VARCHAR) AS domain,
      campaign_names,
      CAST(NULL AS VARCHAR) AS job_id,
      message_id,
      message_content,
      platform,
      status,
      request_id,
      app_name,
      message_name,
      message_opened,
      service_response,
      template,
      contact_key,
      device_id
    FROM src_sfmc_data.mobile_data
    where time > $ { td.last_results.last_session_time }
      and to_base64url(
        xxhash64(
          cast(
            coalesce(message_id, '') || coalesce(request_id, '')
          )
        )
      ) is not null
  )