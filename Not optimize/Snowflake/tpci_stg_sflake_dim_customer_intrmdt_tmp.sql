-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=2=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sflake_dim_customer_intrmdt_tmp;
CREATE TABLE tpci_stg.sflake_dim_customer_intrmdt_tmp AS WITH sflake_dim_customer_1 AS (
	SELECT *
	FROM src_snowflake.dim_customer_temp
),
sflake_dim_customer_2 AS (
	SELECT customer_key AS customer_key,
		days_between_last_two_orders AS days_between_last_two_orders,
		second_to_last_order_date AS second_to_last_order_date,
		second_to_last_order_date_unixtime AS second_to_last_order_date_unixtime,
		email AS email,
		phone AS phone,
		name AS name,
		street AS street,
		city AS city,
		postcode AS postcode,
		region AS region,
		country AS country,
		last_order_date_utc AS last_order_date_utc,
		last_order_date_utc_unixtime AS last_order_date_utc_unixtime,
		first_order_date_utc AS first_order_date_utc,
		first_order_date_utc_unixtime AS first_order_date_utc_unixtime,
		phone_std0 AS phone_std0,
		email_domain AS email_domain,
		first_name AS first_name,
		time AS time
	FROM (
			SELECT customer_key AS customer_key,
				days_between_last_two_orders AS days_between_last_two_orders,
				second_to_last_order_date AS second_to_last_order_date,
				second_to_last_order_date_unixtime AS second_to_last_order_date_unixtime,
				email AS email,
				phone AS phone,
				name AS name,
				street AS street,
				city AS city,
				postcode AS postcode,
				region AS region,
				country AS country,
				last_order_date_utc AS last_order_date_utc,
				last_order_date_utc_unixtime AS last_order_date_utc_unixtime,
				first_order_date_utc AS first_order_date_utc,
				first_order_date_utc_unixtime AS first_order_date_utc_unixtime,
				phone_std0 AS phone_std0,
				email_domain AS email_domain,
				first_name AS first_name,
				time AS time,
				ROW_NUMBER() OVER (
					PARTITION BY customer_key
					ORDER BY time desc
				) rn
			FROM sflake_dim_customer_1
		) a
	WHERE rn = 1
),
sflake_dim_customer_3 AS (
	SELECT sflake_dim_customer_2.city AS city,
		sflake_dim_customer_2.country AS country,
		sflake_dim_customer_2.customer_key AS customer_key,
		sflake_dim_customer_2.days_between_last_two_orders AS days_between_last_two_orders,
		sflake_dim_customer_2.email AS email,
		sflake_dim_customer_2.email_domain AS email_domain,
		sflake_dim_customer_2.first_name AS first_name,
		sflake_dim_customer_2.first_order_date_utc AS first_order_date_utc,
		sflake_dim_customer_2.first_order_date_utc_unixtime AS first_order_date_utc_unixtime,
		sflake_dim_customer_2.last_order_date_utc AS last_order_date_utc,
		sflake_dim_customer_2.last_order_date_utc_unixtime AS last_order_date_utc_unixtime,
		sflake_dim_customer_2.name AS name,
		sflake_dim_customer_2.phone AS phone,
		sflake_dim_customer_2.phone_std0 AS phone_std0,
		sflake_dim_customer_2.postcode AS postcode,
		sflake_dim_customer_2.region AS region,
		sflake_dim_customer_2.second_to_last_order_date AS second_to_last_order_date,
		sflake_dim_customer_2.second_to_last_order_date_unixtime AS second_to_last_order_date_unixtime,
		sflake_dim_customer_2.street AS street,
		src_snowflake_fact_customer_temp.temp_customer AS temp_customer,
		sflake_dim_customer_2.time AS time
	FROM sflake_dim_customer_2 sflake_dim_customer_2
		left join src_snowflake.fact_customer_temp src_snowflake_fact_customer_temp ON sflake_dim_customer_2.customer_key = src_snowflake_fact_customer_temp.customer_key
),
sub_sflake_dim_customer AS (
	SELECT customer_key AS customer_key,
		postcode AS postcode,
		first_order_date_utc AS first_order_date_utc,
		second_to_last_order_date AS second_to_last_order_date,
		email AS email,
		first_order_date_utc_unixtime AS first_order_date_utc_unixtime,
		days_between_last_two_orders AS days_between_last_two_orders,
		email_domain AS email_domain,
		street AS street,
		first_name AS first_name,
		CASE
			WHEN email_domain in (
				select exception_value
				from src_snowflake.exception_list_email
			) then null
			else concat(lower(trim(email)), ':', coalesce(first_name, ''))
		end AS email_std_fname,
		phone_std0 AS phone_std0,
		name AS name,
		CASE
			WHEN phone_std0 in (
				select exception_value
				from src_snowflake.exception_list_phone
			) then null
			else lower(trim(phone_std0))
		end AS phone_std,
		last_order_date_utc AS last_order_date_utc,
		case
			when temp_customer is null then 'No'
			else 'Yes'
		end AS buyer_flag,
		city AS city,
		second_to_last_order_date_unixtime AS second_to_last_order_date_unixtime,
		CASE
			WHEN phone_std0 in (
				select exception_value
				from src_snowflake.exception_list_phone
			) then null
			else concat(
				lower(trim(phone_std0)),
				':',
				coalesce(first_name, '')
			)
		end AS phone_std_fname,
		time AS time,
		last_order_date_utc_unixtime AS last_order_date_utc_unixtime,
		country AS country,
		phone AS phone,
		temp_customer AS temp_customer,
		region AS region,
		CASE
			WHEN email_domain in (
				select exception_value
				from src_snowflake.exception_list_email
			) then null
			else lower(trim(email))
		end AS email_std,
		row_number() over (
			partition by email
			order by time
		) AS rnk
	FROM sflake_dim_customer_3
)
select *
from sub_sflake_dim_customer