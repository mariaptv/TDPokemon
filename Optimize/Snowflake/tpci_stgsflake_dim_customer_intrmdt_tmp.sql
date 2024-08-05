DROP TABLE IF EXISTS tpci_stg.sflake_dim_customer_intrmdt_tmp;

CREATE TABLE tpci_stg.sflake_dim_customer_intrmdt_tmp AS
WITH sflake_dim_customer_deduped AS (

  SELECT *
  FROM (

    SELECT
        customer_key
      , days_between_last_two_orders
      , second_to_last_order_date
      , second_to_last_order_date_unixtime
      , email
      , phone
      , name
      , street
      , city
      , postcode
      , region
      , country
      , last_order_date_utc
      , last_order_date_utc_unixtime
      , first_order_date_utc
      , first_order_date_utc_unixtime
      , phone_std0
      , email_domain
      , first_name
      , time as order_time
      , ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY time DESC) AS rn
    
    FROM src_snowflake.dim_customer_temp

    )
  WHERE rn = 1

)
SELECT 
    customer.customer_key
  , customer.postcode
  , customer.first_order_date_utc
  , customer.second_to_last_order_date
  , customer.email
  , customer.first_order_date_utc_unixtime
  , customer.days_between_last_two_orders
  , customer.email_domain
  , customer.street
  , customer.first_name
  , CASE
      WHEN email_except.email_domain IS NOT NULL THEN NULL
      ELSE LOWER(TRIM(customer.email)) || ':' || COALESCE(customer.first_name, '')
    END AS email_std_fname
  , customer.phone_std0
  , customer.name
  , CASE 
      WHEN phone_except.phone_number IS NOT NULL THEN NULL
      ELSE LOWER(TRIM(customer.phone_std0))
    END AS phone_std
  , customer.last_order_date_utc
  , IF(fact.temp_customer IS NULL, 'No', 'Yes') AS buyer_flag
  , customer.city
  , customer.second_to_last_order_date_unixtime
  , CASE 
      WHEN phone_except.phone_number IS NOT NULL THEN NULL
      ELSE LOWER(TRIM(customer.phone_std0)) || ':' || COALESCE(customer.first_name, '')
    END AS phone_std_fname
  , customer.last_order_date_utc_unixtime
  , customer.country
  , customer.phone
  , fact.temp_customer
  , customer.region
  , CASE 
      WHEN email_except.email_domain IS NOT NULL THEN NULL
      ELSE LOWER(TRIM(customer.email)) 
    END AS email_std
  , ROW_NUMBER() OVER (PARTITION BY email ORDER BY time) AS rnk
  , time as time

FROM sflake_dim_customer_deduped customer

LEFT JOIN src_snowflake.fact_customer_temp fact
  ON fact.customer_key = customer.customer_key

LEFT JOIN (

  SELECT exception_value AS email_domain
  FROM src_snowflake.exception_list_email

) email_except
  ON email_except.email_domain = customer.email_domain

LEFT JOIN (

  SELECT exception_value AS phone_number
  FROM src_snowflake.exception_list_phone

) phone_except
  ON phone_except.phone_number = customer.phone_std0