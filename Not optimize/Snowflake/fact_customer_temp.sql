-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_fact_customer_temp
DROP TABLE IF EXISTS "fact_customer_temp";
CREATE TABLE "fact_customer_temp" AS
SELECT DISTINCT customer_key ,'1' as temp_customer
FROM src_snowflake.fact_order