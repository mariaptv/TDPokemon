-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer0+create_pageviews_onetrust_consent_compliance_preferences_tmp
DROP TABLE IF EXISTS "pageviews_onetrust_consent_compliance_preferences_tmp";

CREATE TABLE
    "pageviews_onetrust_consent_compliance_preferences_tmp" AS
select
    *
from
    src_pcenter_webtracking.pageviews
where
    onetrust_consent_event = 1