DROP TABLE IF EXISTS tpci_stg.sfmc_mobile_intrmdt_tmp;

CREATE TABLE tpci_stg.sfmc_mobile_intrmdt_tmp AS
WITH sub_sfmc_mobile AS (
    SELECT 
        platform,
        request_id,
        ios_media_url,
        open_date,
        push_job_id,
        inbox_message_downloaded,
        service_response,
        message_id,
        message_name,
        android_media_url,
        contact_key,
        media_alt,
        status,
        TD_TIME_PARSE(open_date) AS open_date_unixtime,
        campaigns,
        time_in_app,
        template,
        inbox_message_opened,
        date_time_send,
        page_name,
        campaign_names,
        message_opened,
        time,
        TD_TIME_PARSE(date_time_send) AS date_time_send_unixtime,
        format,
        platform_version,
        device_id,
        TRIM(app_name) AS app_name,
        geofence_name,
        message_content
    FROM src_sfmc_data. 
)
SELECT * FROM sub_sfmc_mobile;
