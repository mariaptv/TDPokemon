-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+handle_sfmc_td_users+check_num^sub+create_td_users_backup
DROP TABLE IF EXISTS "td_users_backup";
CREATE TABLE "td_users_backup" AS
SELECT email,
    opt_in,
    member_id,
    pcenter_email_opt_in,
    last_modified,
    optin_date,
    days_opted_in,
    most_recent_click_date,
    most_recent_click_message_name,
    most_recent_open_date,
    most_recent_open_message_name,
    apple_user,
    subscriber_status,
    total_opens_last_24_months,
    total_clicks_last_24_Months,
    unsubscribe_date,
    domain,
    first_name,
    last_name,
    dob,
    ptc_guid
FROM src_sfmc_data.td_users