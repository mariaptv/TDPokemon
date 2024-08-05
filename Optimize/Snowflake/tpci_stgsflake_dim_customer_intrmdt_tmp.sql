WITH
  a AS (
    SELECT
      customer_key,
      order_date,
      DENSE_RANK() OVER (
        PARTITION BY
          customer_key
        ORDER BY
          order_date DESC
      ) AS order_date_rn
    FROM
      (
        SELECT
          SUBSTRING(order_timestamp_utc, 1, 10) AS order_date,
          customer_key
        FROM
          src_snowflake.fact_order
      )
  ),
  sflake_dim_customer_deduped AS (
    SELECT
      *
    FROM
      (
        SELECT
          customer_key,
          days_between_last_two_orders,
          second_to_last_order_date,
          second_to_last_order_date_unixtime,
          email,
          phone,
          name,
          street,
          city,
          postcode,
          region,
          country,
          last_order_date_utc,
          last_order_date_utc_unixtime,
          first_order_date_utc,
          first_order_date_utc_unixtime,
          phone_std0,
          email_domain,
          first_name,
          time as order_time,
          ROW_NUMBER() OVER (
            PARTITION BY
              customer_key
            ORDER BY
              time DESC
          ) AS rn
        FROM
          (
            SELECT
              customer.customer_key,
              DATE_DIFF (
                'day',
                CAST(second_to_last_order_date.order_date AS DATE),
                CAST(
                  SUBSTRING(customer.last_order_date_utc, 1, 10) AS DATE
                )
              ) AS days_between_last_two_orders,
              second_to_last_order_date.order_date,
              TD_TIME_PARSE (second_to_last_order_date.order_date) AS second_to_last_order_date_unixtime,
              IF (customer.email LIKE '%@%', customer.email, NULL) AS email,
              customer.phone,
              customer.name,
              customer.street,
              customer.city,
              customer.postcode,
              customer.region,
              customer.country,
              customer.last_order_date_utc,
              TD_TIME_PARSE (SUBSTRING(customer.last_order_date_utc, 1, 10)) AS last_order_date_utc_unixtime,
              customer.first_order_date_utc,
              TD_TIME_PARSE (SUBSTRING(customer.first_order_date_utc, 1, 10)) AS first_order_date_utc_unixtime,
              SUBSTRING(
                REGEXP_REPLACE (phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'),
                1,
                20
              ) AS phone_std0,
              REGEXP_EXTRACT (customer.email, '.+@(.+)', 1) AS email_domain,
              SPLIT_PART (LOWER(customer.name), ' ', 1) AS first_name
            FROM
              src_snowflake.dim_customer customer
              LEFT JOIN (
                SELECT DISTINCT
                  order_date,
                  customer_key
                FROM
                  A
                WHERE
                  order_date_rn = 2
              ) second_to_last_order_date ON customer.customer_key = second_to_last_order_date.customer_key
            ORDER BY
              customer.customer_key
          )
      )
    WHERE
      rn = 1
  )
SELECT
  customer.customer_key,
  customer.postcode,
  customer.first_order_date_utc,
  customer.second_to_last_order_date,
  customer.email,
  customer.first_order_date_utc_unixtime,
  customer.days_between_last_two_orders,
  customer.email_domain,
  customer.street,
  customer.first_name,
  CASE
    WHEN email_except.email_domain IS NOT NULL THEN NULL
    ELSE LOWER(TRIM(customer.email)) || ':' || COALESCE(customer.first_name, '')
  END AS email_std_fname,
  customer.phone_std0,
  customer.name,
  CASE
    WHEN phone_except.phone_number IS NOT NULL THEN NULL
    ELSE LOWER(TRIM(customer.phone_std0))
  END AS phone_std,
  customer.last_order_date_utc,
  IF (fact.temp_customer IS NULL, 'No', 'Yes') AS buyer_flag,
  customer.city,
  customer.second_to_last_order_date_unixtime,
  CASE
    WHEN phone_except.phone_number IS NOT NULL THEN NULL
    ELSE LOWER(TRIM(customer.phone_std0)) || ':' || COALESCE(customer.first_name, '')
  END AS phone_std_fname,
  customer.last_order_date_utc_unixtime,
  customer.country,
  customer.phone,
  fact.temp_customer,
  customer.region,
  CASE
    WHEN email_except.email_domain IS NOT NULL THEN NULL
    ELSE LOWER(TRIM(customer.email))
  END AS email_std,
  ROW_NUMBER() OVER (
    PARTITION BY
      email
    ORDER BY
      time
  ) AS rnk,
  time as time
FROM
  sflake_dim_customer_deduped customer
  LEFT JOIN (
    SELECT
      customer_key,
      '1' AS temp_customer
    FROM
      src_snowflake.fact_order
    GROUP BY
      customer_key
  ) fact ON fact.customer_key = customer.customer_key
  LEFT JOIN (
    SELECT
      exception_value AS email_domain
    FROM
      src_snowflake.exception_list_email
  ) email_except ON email_except.email_domain = customer.email_domain
  LEFT JOIN (
    SELECT
      exception_value AS phone_number
    FROM
      src_snowflake.exception_list_phone
  ) phone_except ON phone_except.phone_number = customer.phone_std0