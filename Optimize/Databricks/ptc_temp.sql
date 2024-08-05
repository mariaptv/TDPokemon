DROP
  TABLE
    IF EXISTS "ptc_temp";

CREATE
  TABLE "ptc_temp" AS 
  SELECT 
      guid,
      member_id,
      address,
      birth_date,
      children_updated_timestamp,
      IF(email LIKE '%@%', email, NULL) AS email,
      IF(go_terms = 1, 'true', 'false') AS go_terms,
      nintendo_ptcs_ppid,
      nintendo_ptcs_ppid_updated_timestamp,
      phone,
      IF(play_pokemon_terms = 1, 'true', 'false') AS play_pokemon_terms,
      player_id,
      player_rankings_display,
      screen_name,
      username,
      children,
      IF(first_name IS NULL OR first_name = 'NULL', '', first_name) AS first_name,
      IF(last_name IS NULL OR last_name = 'NULL', '', last_name) AS last_name,
      IF(email_opt_in = 1, 'true', 'false') AS email_opt_in,
      IF(email_opt_in_pcenter = 1, 'true', 'false') AS email_opt_in_pcenter, 
      CAST(
        json_extract_scalar(
          address,
          '$.country'
        ) AS VARCHAR ) AS country,
      REGEXP_EXTRACT(email, '.+@(.+)', 1) AS email_domain,
      SUBSTRING(REGEXP_REPLACE(phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'), 1, 20) AS phone_std0,
      TD_TIME_PARSE(
        SUBSTRING(nintendo_ptcs_ppid_updated_timestamp,
          1,
          19)
      ) AS nintendo_ptcs_ppid_updated_timestamp_unixtime,
      TD_TIME_PARSE(
        SUBSTRING(children_updated_timestamp,
          1,
          19)
      ) AS children_updated_timestamp_unixtime,
      TD_TIME_PARSE(
        SUBSTRING(birth_date,
          1,
          19)
      ) AS birth_date_unixtime
    FROM
      src_databricks.ptc