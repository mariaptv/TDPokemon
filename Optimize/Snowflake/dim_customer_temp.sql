
CREATE TABLE "dim_customer_temp" AS
WITH a AS (
  
  SELECT
      customer_key
    , order_date
    , DENSE_RANK() OVER (PARTITION BY customer_key ORDER BY order_date DESC) AS order_date_rn
  
  FROM (
    SELECT
        SUBSTRING(order_timestamp_utc, 1, 10) AS order_date
      , customer_key
    FROM src_snowflake.fact_order
  )
)
SELECT
    customer.customer_key
  , DATE_DIFF(
      'day',
      CAST(second_to_last_order_date.order_date AS DATE),
      CAST(SUBSTRING(customer.last_order_date_utc, 1, 10) AS DATE)
    ) AS days_between_last_two_orders
  , second_to_last_order_date.order_date
  , TD_TIME_PARSE(second_to_last_order_date.order_date) AS second_to_last_order_date_unixtime
  , IF(customer.email LIKE '%@%', customer.email, NULL) AS email
  , customer.phone
  , customer.name
  , customer.street
  , customer.city
  , customer.postcode
  , customer.region
  , customer.country
  , customer.last_order_date_utc
  , TD_TIME_PARSE(SUBSTRING(customer.last_order_date_utc, 1, 10)) AS last_order_date_utc_unixtime
  , customer.first_order_date_utc
  , TD_TIME_PARSE(SUBSTRING(customer.first_order_date_utc, 1, 10)) AS first_order_date_utc_unixtime
  , SUBSTRING(REGEXP_REPLACE(phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'), 1, 20) AS phone_std0
  , REGEXP_EXTRACT(customer.email, '.+@(.+)', 1) AS email_domain
  , SPLIT_PART(LOWER(customer.name), ' ', 1) AS first_name

FROM src_snowflake.dim_customer customer

LEFT JOIN (

  SELECT DISTINCT
      order_date
    , customer_key
  FROM A
  WHERE order_date_rn = 2

) second_to_last_order_date
  ON customer.customer_key = second_to_last_order_date.customer_key

ORDER BY customer.customer_key