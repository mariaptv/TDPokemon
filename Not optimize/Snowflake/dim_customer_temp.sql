-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_dim_customer_temp
DROP TABLE IF EXISTS "dim_customer_temp";
CREATE TABLE "dim_customer_temp" AS with a as (
   SELECT SUBSTRING(order_timestamp_utc, 1, 10) as order_date,
      customer_key
   from src_snowflake.fact_order
),
b as (
   SELECT DENSE_RANK() OVER (
         PARTITION BY customer_key
         ORDER BY order_date desc
      ) as order_date_rn,
      order_date,
      customer_key
   from a
),
c as (
   SELECT order_date as second_to_last_order_date,
      customer_key
   FROM b
   where order_date_rn = 2
   GROUP BY order_date,
      customer_key
)
SELECT d.customer_key,
   DATE_DIFF(
      'day',
      CAST(c.second_to_last_order_date as DATE),
      cast(SUBSTRING(last_order_date_utc, 1, 10) as DATE)
   ) as days_between_last_two_orders,
   second_to_last_order_date,
   td_time_parse(second_to_last_order_date) as second_to_last_order_date_unixtime,
CASE
      WHEN email like '%@%' THEN email
      ELSE null
   END as email,
   phone,
   name,
   street,
   city,
   postcode,
   region,
   country,
   last_order_date_utc,
   td_time_parse(SUBSTRING(last_order_date_utc, 1, 10)) as last_order_date_utc_unixtime,
   first_order_date_utc,
   td_time_parse(SUBSTRING(first_order_date_utc, 1, 10)) as first_order_date_utc_unixtime,
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
CASE
      WHEN email like '%@%' THEN split_part(lower(email), '@', 2)
      ELSE null
   END as email_domain,
   split_part(lower(name), ' ', 1) as first_name
FROM src_snowflake.dim_customer d
   LEFT JOIN c ON d.customer_key = c.customer_key
order by d.customer_key