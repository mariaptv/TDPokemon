-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_pcen_ecomm_events_temp
DROP TABLE IF EXISTS "pcen_ecomm_events_temp";

CREATE TABLE
    "pcen_ecomm_events_temp" AS
SELECT
    td_referrer,
    td_path,
    td_host,
    td_platform,
    td_user_agent,
    td_url,
    td_description,
    td_title,
    td_viewport,
    td_screen,
    td_color,
    td_language,
    td_charset,
    td_client_id,
    td_version,
    storefront,
    td_ecomm_event_data,
    td_ecomm_event_type,
    amp_cart_id,
    onetrust_consent_id,
    onetrust_functional_cookie,
    onetrust_marketing_cookie,
    onetrust_analytics_cookie,
    onetrust_datalayer_values,
    correlation_id,
    logged_in,
    td_global_id,
    td_ip,
    product_stock_status,
    customer_uid,
    order_id,
    ep_cart_id
FROM
    src_pcenter_webtracking.pcen_ecomm_events
where
    td_ecomm_event_type = 'oneTrustGroupsUpdated'