DROP TABLE IF EXISTS "pageviews_onetrust_consent_compliance_preferences_tmp";

CREATE TABLE
    "pageviews_onetrust_consent_compliance_preferences_tmp" AS
select
    *
from
    src_pcenter_webtracking.pageviews
where
    onetrust_consent_event = 1