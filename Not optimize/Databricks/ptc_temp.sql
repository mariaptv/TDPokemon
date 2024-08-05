-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+handle_ptc+create_ptc_temp
DROP TABLE IF EXISTS "ptc_temp";
CREATE TABLE "ptc_temp" AS
SELECT guid,
    member_id,
    address,
    birth_date,
    children_updated_timestamp,
    CASE
        WHEN email like '%@%' THEN email
        ELSE null
    END as email,
    CASE
        WHEN go_terms = 1 THEN 'true'
        ELSE 'false'
    END as go_terms,
    nintendo_ptcs_ppid,
    nintendo_ptcs_ppid_updated_timestamp,
    phone,
    CASE
        WHEN play_pokemon_terms = 1 THEN 'true'
        ELSE 'false'
    END as play_pokemon_terms,
    player_id,
    player_rankings_display,
    screen_name,
    username,
    children,
    case
        when first_name is null
        or first_name = 'NULL' THEN ''
        ELSE first_name
    end as first_name,
    case
        when last_name is null
        or last_name = 'NULL' THEN ''
        ELSE last_name
    end as last_name,
    CASE
        WHEN email_opt_in = 1 THEN 'true'
        ELSE 'false'
    END as email_opt_in,
    CASE
        WHEN email_opt_in_pcenter = 1 THEN 'true'
        ELSE 'false'
    END as email_opt_in_pcenter,
    CAST(
        json_extract_scalar(address, '$.country') as VARCHAR
    ) as country,
CASE
        WHEN email like '%@%' THEN lower(split_part(email, '@', 2))
        ELSE null
    END as email_domain,
    CASE
        WHEN phone like '+1%' THEN trim(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(REPLACE(REPLACE(phone, '+1'), '-'), '.'),
                                '#'
                            ),
                            ','
                        ),
                        ')'
                    ),
                    '('
                ),
                ' '
            )
        )
        WHEN phone like '1%' THEN trim(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(REPLACE(SUBSTRING(phone, 2, 20), '-'), '.'),
                                '#'
                            ),
                            ','
                        ),
                        ')'
                    ),
                    '('
                ),
                ' '
            )
        )
        ELSE trim(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(REPLACE(REPLACE(phone, '+'), '-'), '.'),
                                '#'
                            ),
                            ','
                        ),
                        ')'
                    ),
                    '('
                ),
                ' '
            )
        )
    END as phone_std0,
    TD_TIME_PARSE(
        SUBSTRING(nintendo_ptcs_ppid_updated_timestamp, 1, 19)
    ) as nintendo_ptcs_ppid_updated_timestamp_unixtime,
    TD_TIME_PARSE(SUBSTRING(children_updated_timestamp, 1, 19)) as children_updated_timestamp_unixtime,
    TD_TIME_PARSE(SUBSTRING(birth_date, 1, 19)) as birth_date_unixtime
from src_databricks.ptc