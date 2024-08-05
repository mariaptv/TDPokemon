-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+handle_ptc+deduplicate_ptc_temp_step1
INSERT OVERWRITE TABLE `ptc_deduped1`
SELECT guid,
    member_id,
    address,
    birth_date,
    children_updated_timestamp,
    email,
    go_terms,
    nintendo_ptcs_ppid,
    nintendo_ptcs_ppid_updated_timestamp,
    phone,
    play_pokemon_terms,
    player_id,
    player_rankings_display,
    screen_name,
    username,
    children,
    first_name,
    last_name,
    email_opt_in,
    email_opt_in_pcenter,
    country,
    email_domain,
    phone_std0,
    nintendo_ptcs_ppid_updated_timestamp_unixtime,
    children_updated_timestamp_unixtime,
    birth_date_unixtime,
    ROW_NUMBER() OVER (
        PARTITION BY guid
        ORDER BY time desc
    ) as RN
from src_databricks.ptc_temp