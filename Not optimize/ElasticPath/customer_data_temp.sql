-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_customer_data_temp
DROP TABLE IF EXISTS "customer_data_temp";
CREATE TABLE "customer_data_temp" AS
SELECT customer_uid,
case
        when cp_first_name is null
        or cp_first_name = 'NULL' THEN ''
        ELSE cp_first_name
    end as cp_first_name,
    case
        when cp_last_name is null
        or cp_last_name = 'NULL' THEN ''
        ELSE cp_last_name
    end as cp_last_name,
CASE
        WHEN cp_email like '%@%' THEN cp_email
        ELSE null
    END as cp_email,
    cp_phone,
    customer_type,
    status,
    storecode,
    creation_date,
    customer_segment_name,
    billing_first_name,
    billing_last_name,
    billing_street_1,
    billing_street_2,
    billing_city,
    billing_sub_country,
    billing_country,
    billing_zip_postal_code,
CASE
        WHEN cp_phone like '+1%' THEN trim(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(REPLACE(REPLACE(cp_phone, '+1'), '-'), '.'),
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
        WHEN cp_phone like '1%' THEN trim(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(REPLACE(SUBSTRING(cp_phone, 2, 20), '-'), '.'),
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
                                REPLACE(REPLACE(REPLACE(cp_phone, '+'), '-'), '.'),
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
CASE
        WHEN cp_email like '%@%' THEN split_part(lower(cp_email), '@', 2)
        ELSE null
    END as email_domain,
    last_modified_date,
    TD_TIME_PARSE(SUBSTRING(creation_date, 1, 19)) as creation_date_unixtime,
    TD_TIME_PARSE(SUBSTRING(last_modified_date, 1, 19)) as last_modified_date_unixtime
from src_elastic_path_data.customer_data