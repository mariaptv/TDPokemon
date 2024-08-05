DROP TABLE IF EXISTS "customer_data_temp";

CREATE TABLE customer_data_temp AS
SELECT 
    customer_uid,
    COALESCE(NULLIF(cp_first_name, 'NULL'), '') AS cp_first_name,
    COALESCE(NULLIF(cp_last_name, 'NULL'), '') AS cp_last_name,
    IF(cp_email LIKE '%@%', cp_email, NULL) AS cp_email,
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
    SUBSTRING(REGEXP_REPLACE(cp_phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'), 1, 20) AS phone_std0,
    REGEXP_EXTRACT(cp_email, '.+@(.+)', 1) AS email_domain,
    last_modified_date,
    TD_TIME_PARSE(SUBSTRING(creation_date, 1, 19)) AS creation_date_unixtime,
    TD_TIME_PARSE(SUBSTRING(last_modified_date, 1, 19)) AS last_modified_date_unixtime
FROM 
    src_elastic_path_data.customer_data;