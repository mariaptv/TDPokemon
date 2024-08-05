-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=12=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.td_sdk_pageviews_intrmdt_tmp;

CREATE TABLE
    tpci_stg.td_sdk_pageviews_intrmdt_tmp AS
WITH
    td_sdk_pageviews_1 AS (
        SELECT
            *
        FROM
            src_pcenter_webtracking.pageviews_temp
    ),
    td_sdk_pageviews_2 AS (
        SELECT
            td_host AS td_host,
            td_description AS td_description,
            td_charset AS td_charset,
            td_url AS td_url,
            trim(onetrust_datalayer_values) AS pcen_onetrust_datalayer_values,
            onetrust_marketing_cookie AS onetrust_marketing_cookie,
            CASE
                WHEN td_url like '%video-game%' THEN 'true'
                else 'false'
            end AS video_game_fan,
            td_screen AS td_screen,
            td_color AS td_color,
            correlation_id AS correlation_id,
            trim(onetrust_functional_cookie) AS pcen_onetrust_functional_cookie,
            onetrust_datalayer_values AS onetrust_datalayer_values,
            td_language AS td_language,
            onetrust_functional_cookie AS onetrust_functional_cookie,
            time AS time,
            customer_uid AS customer_uid,
            td_client_id AS td_client_id,
            cast(time as BIGINT) AS web_activity_time,
            case
                when td_client_id is not null then concat (
                    lower(trim(td_client_id)),
                    ':',
                    coalesce(cast(customer_uid as varchar), '')
                )
                else null
            end AS td_client_cust_uid,
            storefront AS storefront,
            case
                when td_global_id is not null then concat (
                    lower(trim(td_global_id)),
                    ':',
                    coalesce(cast(customer_uid as varchar), '')
                )
                else null
            end AS td_global_cust_uid,
            td_global_id AS td_global_id,
            td_ip AS td_ip,
            td_version AS td_version,
            td_user_agent AS td_user_agent,
            logged_in AS logged_in,
            ep_cart_id AS ep_cart_id,
            td_referrer AS td_referrer,
            trim(correlation_id) AS pcen_correlation_id,
            trim(onetrust_marketing_cookie) AS pcen_onetrust_marketing_cookie,
            onetrust_analytics_cookie AS onetrust_analytics_cookie,
            cast(customer_uid as BIGINT) AS pcen_ep_customer_uid,
            onetrust_consent_id AS onetrust_consent_id,
            td_viewport AS td_viewport,
            td_title AS td_title,
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
            td_path AS td_path,
            trim(onetrust_analytics_cookie) AS pcen_onetrust_analytics_cookie,
            td_platform AS td_platform
        FROM
            td_sdk_pageviews_1
    ),
    sub_td_sdk_pageviews AS (
        SELECT
            td_url AS td_url,
            td_language AS td_language,
            logged_in AS logged_in,
            pcen_ep_customer_uid AS pcen_ep_customer_uid,
            storefront AS storefront,
            ep_cart_id AS ep_cart_id,
            td_referrer AS td_referrer,
            web_activity_time AS web_activity_time,
            td_ip AS td_ip,
            tcg_fan AS tcg_fan,
            td_path AS td_path,
            pcen_onetrust_analytics_cookie AS pcen_onetrust_analytics_cookie,
            td_platform AS td_platform,
            video_game_fan AS video_game_fan,
            td_host AS td_host,
            
            pcen_onetrust_functional_cookie AS pcen_onetrust_functional_cookie,
            onetrust_consent_id AS onetrust_consent_id,
            td_viewport AS td_viewport,
            td_title AS td_title,
            pcen_onetrust_datalayer_values AS pcen_onetrust_datalayer_values,
            td_client_cust_uid AS td_client_cust_uid,
            customer_uid AS customer_uid,
            td_client_id AS td_client_id,
            td_description AS td_description,
            td_color AS td_color,
            td_global_cust_uid AS td_global_cust_uid,
            td_global_id AS td_global_id,
            td_charset AS td_charset,
            pcen_correlation_id AS pcen_correlation_id,
            time AS time,
            td_screen AS td_screen,
            td_version AS td_version,
            td_user_agent AS td_user_agent,
            pcen_onetrust_marketing_cookie AS pcen_onetrust_marketing_cookie
        FROM
            td_sdk_pageviews_2
    )
select
    *
from
    sub_td_sdk_pageviews