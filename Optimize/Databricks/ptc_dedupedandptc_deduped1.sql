INSERT INTO "ptc_deduped"
SELECT
  guid,
  member_id,
  address,
  birth_date,
  children_updated_timestamp,
  email,
  go_terms,
  nintendo_ptcs_ppid,
  nintendo_ptcs_ppid_updated_timestamp,
  phone,
  play_pokemon_terms,
  player_id,
  player_rankings_display,
  screen_name,
  username,
  children,
  first_name,
  last_name,
  email_opt_in,
  email_opt_in_pcenter,
  country,
  email_domain,
  phone_std0,
  nintendo_ptcs_ppid_updated_timestamp_unixtime,
  children_updated_timestamp_unixtime,
  birth_date_unixtime
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY guid ORDER BY time DESC) AS RN
  FROM src_databricks.ptc_temp
)
WHERE RN = 1;