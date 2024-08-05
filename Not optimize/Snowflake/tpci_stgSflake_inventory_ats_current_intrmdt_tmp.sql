-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=5=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
DROP TABLE IF EXISTS tpci_stg.sflake_inventory_ats_current_intrmdt_tmp; CREATE TABLE tpci_stg.sflake_inventory_ats_current_intrmdt_tmp AS WITH 
sflake_inventory_ats_current_1 AS  (SELECT
  *
FROM
src_snowflake.inventory_ats_current),

sub_sflake_inventory_ats_current AS  (SELECT update_date  AS  update_date,
sku  AS  sku,
time  AS  time,
storefront  AS  storefront,
CASE WHEN storefront ='pokemoncenter.com' THEN 'US' WHEN storefront ='pokemoncenter.com/en-gb'  THEN 'CA' WHEN storefront ='pokemoncenter.com/en-ca'  THEN 'UK' ELSE storefront  END    AS  storefront_2_char_format,
quantity  AS  quantity 
FROM
  sflake_inventory_ats_current_1)

select * from sub_sflake_inventory_ats_current