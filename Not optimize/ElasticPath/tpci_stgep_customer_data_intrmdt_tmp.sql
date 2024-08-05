-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=0=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.ep_customer_data_intrmdt_tmp;
CREATE TABLE tpci_stg.ep_customer_data_intrmdt_tmp AS WITH ep_customer_data_1 AS (
    SELECT *
    FROM src_elastic_path_data.customer_data_temp
),
ep_customer_data_2 AS (
    SELECT customer_uid AS customer_uid,
        cp_first_name AS cp_first_name,
        cp_last_name AS cp_last_name,
        cp_email AS cp_email,
        cp_phone AS cp_phone,
        customer_type AS customer_type,
        status AS status,
        storecode AS storecode,
        creation_date AS creation_date,
        customer_segment_name AS customer_segment_name,
        billing_first_name AS billing_first_name,
        billing_last_name AS billing_last_name,
        billing_street_1 AS billing_street_1,
        billing_street_2 AS billing_street_2,
        billing_city AS billing_city,
        billing_sub_country AS billing_sub_country,
        billing_country AS billing_country,
        billing_zip_postal_code AS billing_zip_postal_code,
        phone_std0 AS phone_std0,
        email_domain AS email_domain,
        last_modified_date AS last_modified_date,
        creation_date_unixtime AS creation_date_unixtime,
        last_modified_date_unixtime AS last_modified_date_unixtime,
        time AS time
    FROM (
            SELECT customer_uid AS customer_uid,
                cp_first_name AS cp_first_name,
                cp_last_name AS cp_last_name,
                cp_email AS cp_email,
                cp_phone AS cp_phone,
                customer_type AS customer_type,
                status AS status,
                storecode AS storecode,
                creation_date AS creation_date,
                customer_segment_name AS customer_segment_name,
                billing_first_name AS billing_first_name,
                billing_last_name AS billing_last_name,
                billing_street_1 AS billing_street_1,
                billing_street_2 AS billing_street_2,
                billing_city AS billing_city,
                billing_sub_country AS billing_sub_country,
                billing_country AS billing_country,
                billing_zip_postal_code AS billing_zip_postal_code,
                phone_std0 AS phone_std0,
                email_domain AS email_domain,
                last_modified_date AS last_modified_date,
                creation_date_unixtime AS creation_date_unixtime,
                last_modified_date_unixtime AS last_modified_date_unixtime,
                time AS time,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_uid
                    ORDER BY last_modified_date desc
                ) rn
            FROM ep_customer_data_1
        ) a
    WHERE rn = 1
),
sub_ep_customer_data AS (
    SELECT CASE
            WHEN lower(storecode) = 'pokemon' THEN 'US'
            WHEN lower(storecode) = 'pokemon-ca' THEN 'CA'
            WHEN lower(storecode) = 'pokemon-uk' THEN 'UK'
            ELSE null
        END AS storefront,
        CASE
            WHEN phone_std0 in (
                select exception_value
                from src_snowflake.exception_list_phone
            ) then null
            else concat(
                lower(trim(phone_std0)),
                ':',
                coalesce(cp_first_name, '')
            )
        end AS phone_std_fname,
        CASE
            WHEN email_domain in (
                select exception_value
                from src_snowflake.exception_list_email
            ) then null
            else lower(trim(cp_email))
        end AS email_std,
        last_modified_date AS last_modified_date,
        billing_street_1 AS billing_street_1,
        CASE
            WHEN email_domain in (
                select exception_value
                from src_snowflake.exception_list_email
            ) then null
            else concat(
                lower(trim(cp_email)),
                ':',
                coalesce(cp_first_name, '')
            )
        end AS email_std_fname,
        cp_first_name AS cp_first_name,
        customer_uid AS customer_uid,
        billing_first_name AS billing_first_name,
        status AS status,
        time AS time,
        billing_country AS billing_country,
        billing_city AS billing_city,
        cp_phone AS cp_phone,
        cp_last_name AS cp_last_name,
        cp_email AS cp_email,
        CASE
            WHEN phone_std0 in (
                select exception_value
                from src_snowflake.exception_list_phone
            ) then null
            else phone_std0
        end AS phone_std,
        customer_type AS customer_type,
        billing_sub_country AS billing_sub_country,
        concat(cp_first_name, ' ', cp_last_name) AS name,
        creation_date_unixtime AS creation_date_unixtime,
        phone_std0 AS phone_std0,
        storecode AS storecode,
        email_domain AS email_domain,
        billing_zip_postal_code AS billing_zip_postal_code,
        creation_date AS creation_date,
        billing_street_2 AS billing_street_2,
        customer_segment_name AS customer_segment_name,
        last_modified_date_unixtime AS last_modified_date_unixtime,
        billing_last_name AS billing_last_name
    FROM ep_customer_data_2
)
select *
from sub_ep_customer_data