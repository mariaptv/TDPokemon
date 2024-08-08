DROP TABLE IF EXISTS tpci_stg.td_sdk_pageviews_intrmdt_tmp;

CREATE TABLE tpci_stg.td_sdk_pageviews_intrmdt_tmp AS
WITH td_sdk_pageviews_optimized AS (
    SELECT 
        td_host,
        td_description,
        td_charset,
        td_url,
        TRIM(onetrust_datalayer_values) AS pcen_onetrust_datalayer_values,
        onetrust_marketing_cookie,
        CASE
                WHEN td_url like '%video-game%' THEN 'true'
                else 'false'
            end AS video_game_fan,
        td_screen,
        td_color,
        correlation_id,
        TRIM(onetrust_functional_cookie) AS pcen_onetrust_functional_cookie,
        onetrust_datalayer_values,
        td_language,
        onetrust_functional_cookie,
        time,
        customer_uid,
        td_client_id,
        CAST(time AS BIGINT) AS web_activity_time,
        case
                when td_client_id is not null then concat (
                    lower(trim(td_client_id)),
                    ':',
                    coalesce(cast(customer_uid as varchar), '')
                )
                else null
            end AS td_client_cust_uid,
        storefront,
         case
                when td_global_id is not null then concat (
                    lower(trim(td_global_id)),
                    ':',
                    coalesce(cast(customer_uid as varchar), '')
                )
                else null
            end AS td_global_cust_uid,
        td_global_id,
        td_ip,
        td_version,
        td_user_agent,
        logged_in,
        ep_cart_id,
        td_referrer,
        TRIM(correlation_id) AS pcen_correlation_id,
        TRIM(onetrust_marketing_cookie) AS pcen_onetrust_marketing_cookie,
        onetrust_analytics_cookie,
        CAST(customer_uid AS BIGINT) AS pcen_ep_customer_uid,
        onetrust_consent_id,
        td_viewport,
        td_title,
        CASE
                WHEN td_url like '%trading-card-game%'
                OR td_url like '%tcg%'
                OR td_url like '%booster-pack%'
                OR td_url like '%elite-trainer-box%'
                OR td_url like '%etb%'
                OR td_url like '%oversize-card-set%'
                OR td_url like '%tcg-accessories%'
                OR td_url like '%card-sleeves%'
                OR td_url like '%deck-boxes%' THEN 'true'
                else 'false'
            end AS tcg_fan,
        td_path,
        TRIM(onetrust_analytics_cookie) AS pcen_onetrust_analytics_cookie,
        td_platform
    FROM src_pcenter_webtracking.pageviews
    where
    time > TO_UNIXTIME (current_timestamp) - 7862400

)

SELECT 
    td_url,
    td_language,
    logged_in,
    pcen_ep_customer_uid,
    storefront,
    ep_cart_id,
    td_referrer,
    web_activity_time,
    td_ip,
    tcg_fan,
    td_path,
    pcen_onetrust_analytics_cookie,
    td_platform,
    video_game_fan,
    td_host,
    pcen_onetrust_functional_cookie,
    onetrust_consent_id,
    td_viewport,
    td_title,
    pcen_onetrust_datalayer_values,
    td_client_cust_uid,
    customer_uid,
    td_client_id,
    td_description,
    td_color,
    td_global_cust_uid,
    td_global_id,
    td_charset,
    pcen_correlation_id,
    time,
    td_screen,
    td_version,
    td_user_agent,
    pcen_onetrust_marketing_cookie
FROM td_sdk_pageviews_optimized;
