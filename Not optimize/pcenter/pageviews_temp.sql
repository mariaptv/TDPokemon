-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_pcenter_pageviews_temp
DROP TABLE IF EXISTS "pageviews_temp";

CREATE TABLE
    "pageviews_temp" AS
select
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
    onetrust_functional_cookie,
    onetrust_marketing_cookie,
    onetrust_analytics_cookie,
    logged_in,
    td_global_id,
    td_ip,
    ep_cart_id,
    customer_uid,
    onetrust_datalayer_values,
    correlation_id,
    storefront,
    onetrust_consent_id,
    time
from
    src_pcenter_webtracking.pageviews
where
    time > TO_UNIXTIME (current_timestamp) - 7862400