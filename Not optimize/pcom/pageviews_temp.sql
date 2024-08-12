-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_pcom_pageviews_temp
DROP TABLE IF EXISTS "pageviews_temp";
CREATE TABLE "pageviews_temp" AS
select td_referrer,td_path,td_host,td_platform,td_user_agent,td_url,td_description,td_title,td_viewport,td_screen,td_color,td_language,td_charset,td_client_id,td_version,td_global_id,td_ip,time from src_pcom_webtracking.pageviews
where time > TO_UNIXTIME(current_timestamp) - 7862400