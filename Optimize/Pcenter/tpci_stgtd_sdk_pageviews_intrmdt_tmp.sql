-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=12=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run

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
        IF(td_url LIKE '%video-game%', 'true', 'false') AS video_game_fan,
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
        IF(td_client_id IS NOT NULL, CONCAT(LOWER(TRIM(td_client_id)) || ':' || COALESCE(CAST(customer_uid AS VARCHAR), '')), NULL) AS td_client_cust_uid,
        storefront,
        IF(td_global_id IS NOT NULL, CONCAT(LOWER(TRIM(td_global_id)), ':', COALESCE(CAST(customer_uid AS VARCHAR), '')), NULL) AS td_global_cust_uid,
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
        IF(td_url LIKE '%trading-card-game%' 
           OR td_url LIKE '%tcg%' 
           OR td_url LIKE '%booster-pack%' 
           OR td_url LIKE '%elite-trainer-box%' 
           OR td_url LIKE '%etb%' 
           OR td_url LIKE '%oversize-card-set%' 
           OR td_url LIKE '%tcg-accessories%' 
           OR td_url LIKE '%card-sleeves%' 
           OR td_url LIKE '%deck-boxes%', 'true', 'false') AS tcg_fan,
        td_path,
        TRIM(onetrust_analytics_cookie) AS pcen_onetrust_analytics_cookie,
        td_platform
    FROM src_pcenter_webtracking.pageviews_temp
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
