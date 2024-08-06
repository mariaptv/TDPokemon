DROP TABLE IF EXISTS tpci_stg.ep_customer_data_intrmdt_tmp;

CREATE TABLE
    tpci_stg.ep_customer_data_intrmdt_tmp AS
WITH
    deduped_customer_data AS (
        SELECT
            *
        FROM
            (
                SELECT
                    cte.customer_uid,
                    COALESCE(NULLIF(cte.cp_first_name, 'NULL'), '') AS cp_first_name,
                    COALESCE(NULLIF(cte.cp_last_name, 'NULL'), '') AS cp_last_name,
                    IF (cp_email LIKE '%@%', cp_email, NULL) AS cp_email,
                    cte.cp_phone,
                    cte.customer_type,
                    cte.status,
                    cte.storecode,
                    cte.creation_date,
                    cte.customer_segment_name,
                    cte.billing_first_name,
                    cte.billing_last_name,
                    cte.billing_street_1,
                    cte.billing_street_2,
                    cte.billing_city,
                    cte.billing_sub_country,
                    cte.billing_country,
                    cte.billing_zip_postal_code,
                    SUBSTRING(
                        REGEXP_REPLACE (cte.cp_phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'),
                        1,
                        20
                    ) AS phone_std0,
                    REGEXP_EXTRACT (cte.cp_email, '.+@(.+)', 1) AS email_domain,
                    cte.last_modified_date,
                    TD_TIME_PARSE (SUBSTRING(cte.creation_date, 1, 19)) AS creation_date_unixtime,
                    TD_TIME_PARSE (SUBSTRING(cte.last_modified_date, 1, 19)) AS last_modified_date_unixtime,
                    time,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            cte.customer_uid
                        ORDER BY
                            cte.last_modified_date DESC
                    ) as rn
                FROM
                    src_elastic_path_data.customer_data cte
            )
        WHERE
            rn = 1
    )
SELECT
    CASE
        WHEN LOWER(customer.storecode) = 'pokemon' THEN 'US'
        WHEN LOWER(customer.storecode) = 'pokemon-ca' THEN 'CA'
        WHEN LOWER(customer.storecode) = 'pokemon-uk' THEN 'UK'
        ELSE NULL
    END AS storefront,
    CASE
        WHEN phone_except.phone_number IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(customer.phone_std0)) || ':' || COALESCE(customer.cp_first_name, '')
    END AS phone_std_fname,
    CASE
        WHEN email_except.email_doman IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(customer.cp_email))
    END AS email_std,
    customer.last_modified_date,
    customer.billing_street_1,
    CASE
        WHEN email_except.email_doman IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(customer.cp_email)) || ':' || COALESCE(customer.cp_first_name, '')
    END AS email_std_fname,
    customer.cp_first_name,
    customer.customer_uid,
    customer.billing_first_name,
    customer.status,
    customer.time,
    customer.billing_country,
    customer.billing_city,
    customer.cp_phone,
    customer.cp_last_name,
    customer.cp_email,
    CASE
        WHEN phone_except.phone_number IS NOT NULL THEN NULL
        ELSE phone_std0
    END AS phone_std,
    customer.customer_type,
    customer.billing_sub_country,
    customer.cp_first_name || ' ' || cp_last_name AS name,
    customer.creation_date_unixtime,
    customer.phone_std0,
    customer.storecode,
    customer.email_domain,
    customer.billing_zip_postal_code,
    customer.creation_date,
    customer.billing_street_2,
    customer.customer_segment_name,
    customer.last_modified_date_unixtime,
    customer.billing_last_name
FROM
    deduped_customer_data customer

    LEFT JOIN (
    SELECT exception_value AS phone_number
    FROM src_snowflake.exception_list_phone
    ) phone_except ON phone_except.phone_number = customer.phone_std0
    LEFT JOIN (
        SELECT
            exception_value AS email_doman
        FROM
            src_snowflake.exception_list_email
    ) email_except ON email_except.email_doman = customer.email_domain