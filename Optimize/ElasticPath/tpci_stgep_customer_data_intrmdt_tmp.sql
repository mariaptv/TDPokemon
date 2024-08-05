DROP TABLE IF EXISTS tpci_stg.ep_customer_data_intrmdt_tmp;
CREATE TABLE tpci_stg.ep_customer_data_intrmdt_tmp AS WITH deduped_customer_data AS (
    SELECT *
    FROM (
            SELECT customer_uid,
                cp_first_name,
                cp_last_name,
                cp_email,
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
                phone_std0,
                email_domain,
                last_modified_date,
                creation_date_unixtime,
                last_modified_date_unixtime,
                time,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_uid
                    ORDER BY last_modified_date DESC
                ) as rn
            FROM src_elastic_path_data.customer_data_temp
        )
    WHERE rn = 1
)
SELECT CASE
        WHEN LOWER(storecode) = 'pokemon' THEN 'US'
        WHEN LOWER(storecode) = 'pokemon-ca' THEN 'CA'
        WHEN LOWER(storecode) = 'pokemon-uk' THEN 'UK'
        ELSE NULL
    END AS storefront,
    CASE
        WHEN phone_except.phone_number IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(phone_std0)) ||':' || COALESCE(cp_first_name, '')
    END AS phone_std_fname,
    CASE
        WHEN email_except.email_domain IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(cp_email))
    END AS email_std,
    last_modified_date,
    billing_street_1,
    CASE
        WHEN email_except.email_domain IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(cp_email)) || ':' || COALESCE(cp_first_name, '')
    END AS email_std_fname,
    cp_first_name,
    customer_uid,
    billing_first_name,
    status,
    time,
    billing_country,
    billing_city,
    cp_phone,
    cp_last_name,
    cp_email,
    CASE
        WHEN phone_except.phone_number IS NOT NULL THEN NULL
        ELSE phone_std0
    END AS phone_std,
    customer_type,
    billing_sub_country,
    cp_first_name || ' '|| cp_last_name AS name,
    creation_date_unixtime,
    phone_std0,
    storecode,
    email_domain,
    billing_zip_postal_code,
    creation_date,
    billing_street_2,
    customer_segment_name,
    last_modified_date_unixtime,
    billing_last_name
FROM deduped_customer_data

LEFT JOIN (

  SELECT exception_value AS phone_number
  FROM src_snowflake.exception_list_phone

) phone_except
  ON phone_except.phone_number = customer.phone_std0

  LEFT JOIN (

  SELECT exception_value AS email_domain
  FROM src_snowflake.exception_list_email

) email_except
  ON email_except.email_domain = customer.email_domain