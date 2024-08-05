-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=7=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.db_ptc_intrmdt_tmp;
CREATE TABLE tpci_stg.db_ptc_intrmdt_tmp AS WITH db_ptc_1 AS (
    SELECT *
    FROM src_databricks.ptc_deduped
),
sub_db_ptc AS (
    SELECT address AS address,
        email_domain AS email_domain,
        email_opt_in_pcenter AS email_opt_in_pcenter,
        first_name AS first_name,
        CASE
            WHEN email_domain in (
                select exception_value
                from src_snowflake.exception_list_email
            ) then null
            else lower(trim(email))
        end AS email_std,
        children_updated_timestamp_unixtime AS children_updated_timestamp_unixtime,
        play_pokemon_terms AS play_pokemon_terms,
        screen_name AS screen_name,
        email_opt_in AS email_opt_in,
        player_id AS player_id,
        children_updated_timestamp AS children_updated_timestamp,
        CASE
            WHEN phone_std0 in (
                select exception_value
                from src_snowflake.exception_list_phone
            ) then null
            else phone_std0
        end AS phone_std,
        time AS time,
        last_name AS last_name,
        children AS children,
        username AS username,
        birth_date_unixtime AS birth_date_unixtime,
        nintendo_ptcs_ppid_updated_timestamp_unixtime AS nintendo_ptcs_ppid_updated_timestamp_unixtime,
        phone_std0 AS phone_std0,
        concat(first_name, ' ', last_name) AS name,
        player_rankings_display AS player_rankings_display,
        guid AS guid,
        country AS country,
        birth_date AS birth_date,
        go_terms AS go_terms,
        phone AS phone,
        nintendo_ptcs_ppid_updated_timestamp AS nintendo_ptcs_ppid_updated_timestamp,
        email AS email,
        nintendo_ptcs_ppid AS nintendo_ptcs_ppid,
        member_id AS member_id,
        CASE
            WHEN email_domain in (
                select exception_value
                from src_snowflake.exception_list_email
            ) then null
            else concat(lower(trim(email)), ':', coalesce(first_name, ''))
        end AS email_std_fname
    FROM db_ptc_1
)
select *
from sub_db_ptc