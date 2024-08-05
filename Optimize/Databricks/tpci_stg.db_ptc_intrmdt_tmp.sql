DROP TABLE IF EXISTS tpci_stg.db_ptc_intrmdt_tmp;

CREATE TABLE tpci_stg.db_ptc_intrmdt_tmp AS
SELECT 
    address,
    email_domain,
    email_opt_in_pcenter,
    first_name,
    CASE
        WHEN email_except.email_domain IS NOT NULL THEN NULL
        ELSE LOWER(TRIM(email))
    END AS email_std,
    children_updated_timestamp_unixtime,
    play_pokemon_terms,
    screen_name,
    email_opt_in,
    player_id,
    children_updated_timestamp,
    CASE
        WHEN phone_except.phone_number IS NOT NULL THEN NULL
        ELSE phone_std0
    END AS phone_std,
    time,
    last_name,
    children,
    username,
    birth_date_unixtime,
    nintendo_ptcs_ppid_updated_timestamp_unixtime,
    phone_std0,
    first_name || ' ' || last_name AS name,
    player_rankings_display,
    guid,
    country,
    birth_date,
    go_terms,
    phone,
    nintendo_ptcs_ppid_updated_timestamp,
    email,
    nintendo_ptcs_ppid,
    member_id,
    CASE
        WHEN email_except.email_domain IS NOT NULL THEN NULL
        ELSE CONCAT(LOWER(TRIM(email)), ':', COALESCE(first_name, ''))
    END AS email_std_fname
FROM src_databricks.ptc_deduped

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