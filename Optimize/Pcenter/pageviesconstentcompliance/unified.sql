CREATE TABLE tpci_stg.td_sdk_pageviews_onetrust_consent_compliance_preferences_intrmdt_tmp AS
WITH latest_consent_preferences AS (
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
        onetrust_consent_event,
        email_hit_event,
        email_hit_sub_id,
        customer_type,
        time,
        ROW_NUMBER() OVER (
            PARTITION BY
                correlation_id
            ORDER BY
                time DESC
        ) AS rn
    FROM (
        SELECT
            *
        FROM
            src_pcenter_webtracking.pageviews
        WHERE
            onetrust_consent_event = 1
    )
)
SELECT *
FROM latest_consent_preferences
WHERE rn = 1;
