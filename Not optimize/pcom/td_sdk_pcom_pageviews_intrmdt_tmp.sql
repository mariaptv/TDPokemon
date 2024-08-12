-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=13=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.td_sdk_pcom_pageviews_intrmdt_tmp;
CREATE TABLE tpci_stg.td_sdk_pcom_pageviews_intrmdt_tmp AS WITH td_sdk_pcom_pageviews_1 AS (
    SELECT *
    FROM src_pcom_webtracking.pageviews_temp
),
sub_td_sdk_pcom_pageviews AS (
    SELECT CASE
            WHEN td_url like '%unite.%'
            OR td_url like '%-unite%'
            OR td_url like '%unite-%' THEN 'true'
            else 'false'
        end AS unite_fan,
        td_referrer AS td_referrer,
        CASE
            WHEN td_url like '%episodes%'
            OR td_url like '%episodi%'
            OR td_url like '%folgen%'
            OR td_url like '%episodios%'
            OR td_url like '%watch.pokemon%'
            OR td_url like '%app/pokemon-tv%'
            OR td_url like '%animation%' THEN 'true'
            else 'false'
        end AS animation_fan,
        CASE
            WHEN td_url like '%play-pokemon%'
            OR td_url like '%events.pokemon.com%' THEN 'true'
            else 'false'
        end AS op_pe_fan,
        CASE
            WHEN td_url like '%tcg%'
            OR td_url like '%jcc-%'
            OR td_url like '%/gcc%'
            OR td_url like '%sammelkartenspiel%' THEN 'true'
            else 'false'
        end AS tcg_fan,
        td_ip AS td_ip,
        td_color AS td_color,
        td_global_id AS td_global_id,
        td_language AS td_language,
        td_path AS td_path,
        td_screen AS td_screen,
        td_description AS td_description,
        td_url AS td_url,
        td_platform AS td_platform,
        td_client_id AS td_client_id,
        td_charset AS td_charset,
        td_viewport AS td_viewport,
        time AS time,
        trim(td_host) AS td_host,
        td_user_agent AS td_user_agent,
        td_version AS td_version,
        td_title AS td_title,
        CASE
            WHEN td_url like '%scarletviolet.pokemon.com%'
            OR td_url like '%teraincursiones%'
            OR td_url like '%raid-teracristal%'
            OR td_url like '%tera‑raids%'
            OR td_url like '%raids-teracristal%'
            OR td_url like '%tera-raid-battles%'
            OR td_url like '%pokemon-scarlet-and-pokemon-violet%'
            OR td_url like '%pokemon-ecarlate-et-pokemon-violet%'
            OR td_url like '%pokemon-scarlatto-e-pokemon-violetto%'
            OR td_url like '%pokemon-karmesin-und-pokemon-purpur%'
            OR td_url like '%pokemon-karmesin-und-pokemon-purpur%'
            OR td_url like '%pokemon-escarlata-y-pokemon-purpura%' THEN 'true'
            else 'false'
        end AS scarlet_violet_fan,
        CASE
            WHEN td_url like '%unite.%'
            OR td_url like '%scarletviolet.%'
            OR td_url like '%jeux-video%'
            OR td_url like '%video-games%'
            OR td_url like '%jeux-video%'
            OR td_url like '%videogiochi%'
            OR td_url like '%videospiele%'
            OR td_url like '%videojuegos%' THEN 'true'
            else 'false'
        end AS video_game_fan
    FROM td_sdk_pcom_pageviews_1
)
select *
from sub_td_sdk_pcom_pageviews