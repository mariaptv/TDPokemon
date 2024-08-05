-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=14=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.td_sdk_pageviews_onetrust_consent_compliance_preferences_intrmdt_tmp; CREATE TABLE tpci_stg.td_sdk_pageviews_onetrust_consent_compliance_preferences_intrmdt_tmp AS WITH 
td_sdk_pageviews_onetrust_consent_compliance_preferences_1 AS  (SELECT
  *
FROM
src_pcenter_webtracking.pageviews_onetrust_consent_compliance_preferences_tmp),

sub_td_sdk_pageviews_onetrust_consent_compliance_preferences AS  (SELECT td_referrer  AS  td_referrer,
td_path  AS  td_path,
td_host  AS  td_host,
td_platform  AS  td_platform,
td_user_agent  AS  td_user_agent,
td_url  AS  td_url,
td_description  AS  td_description,
td_title  AS  td_title,
td_viewport  AS  td_viewport,
td_screen  AS  td_screen,
td_color  AS  td_color,
td_language  AS  td_language,
td_charset  AS  td_charset,
td_client_id  AS  td_client_id,
td_version  AS  td_version,
onetrust_functional_cookie  AS  onetrust_functional_cookie,
onetrust_marketing_cookie  AS  onetrust_marketing_cookie,
onetrust_analytics_cookie  AS  onetrust_analytics_cookie,
logged_in  AS  logged_in,
td_global_id  AS  td_global_id,
td_ip  AS  td_ip,
ep_cart_id  AS  ep_cart_id,
customer_uid  AS  customer_uid,
onetrust_datalayer_values  AS  onetrust_datalayer_values,
correlation_id  AS  correlation_id,
storefront  AS  storefront,
onetrust_consent_id  AS  onetrust_consent_id,
onetrust_consent_event  AS  onetrust_consent_event,
email_hit_event  AS  email_hit_event,
email_hit_sub_id  AS  email_hit_sub_id,
customer_type  AS  customer_type,
time  AS  time 
FROM (
	SELECT td_referrer  AS  td_referrer,
td_path  AS  td_path,
td_host  AS  td_host,
td_platform  AS  td_platform,
td_user_agent  AS  td_user_agent,
td_url  AS  td_url,
td_description  AS  td_description,
td_title  AS  td_title,
td_viewport  AS  td_viewport,
td_screen  AS  td_screen,
td_color  AS  td_color,
td_language  AS  td_language,
td_charset  AS  td_charset,
td_client_id  AS  td_client_id,
td_version  AS  td_version,
onetrust_functional_cookie  AS  onetrust_functional_cookie,
onetrust_marketing_cookie  AS  onetrust_marketing_cookie,
onetrust_analytics_cookie  AS  onetrust_analytics_cookie,
logged_in  AS  logged_in,
td_global_id  AS  td_global_id,
td_ip  AS  td_ip,
ep_cart_id  AS  ep_cart_id,
customer_uid  AS  customer_uid,
onetrust_datalayer_values  AS  onetrust_datalayer_values,
correlation_id  AS  correlation_id,
storefront  AS  storefront,
onetrust_consent_id  AS  onetrust_consent_id,
onetrust_consent_event  AS  onetrust_consent_event,
email_hit_event  AS  email_hit_event,
email_hit_sub_id  AS  email_hit_sub_id,
customer_type  AS  customer_type,
time  AS  time ,ROW_NUMBER() OVER (
			PARTITION BY correlation_id ORDER BY time desc
			) rn
	FROM td_sdk_pageviews_onetrust_consent_compliance_preferences_1
	) a
WHERE rn = 1)

select * from sub_td_sdk_pageviews_onetrust_consent_compliance_preferences