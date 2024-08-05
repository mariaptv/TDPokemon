-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+src^sub+for-0=list=4=%7B%22data%22%3A+run_transforms^sub+build^sub+td-for-each-0+run
WITH 
sflake_tracking_current_1 AS  (SELECT
  *
FROM
src_snowflake.tracking_current),

sub_sflake_tracking_current AS  (SELECT event_state  AS  event_state,
package_carrier_moniker  AS  package_carrier_moniker,
event_date  AS  event_date,
fulfillment_id  AS  fulfillment_id,
dest_city  AS  dest_city,
time  AS  time,
status_code_description  AS  status_code_description,
service_description  AS  service_description,
dest_state  AS  dest_state,
tracking_number  AS  tracking_number,
trim(dest_country)   AS  dest_country,
event_country  AS  event_country,
dest_zipcode  AS  dest_zipcode,
event_city  AS  event_city 
FROM
  sflake_tracking_current_1)

INSERT OVERWRITE TABLE tpci_stg.sflake_tracking_current_intrmdt_tmp
select * from sub_sflake_tracking_current