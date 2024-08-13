-- Ask exception list for email
-- Need to create a standardized event name for each event like pageview, click, add to cart, etc.
-- Should we assume some atributes as the same or try to find them? Check if uuid from amplitude matches customer_uid. If so, map. If no, new field.
-- I dont understand this: All TD JS SDK fields. Map fields from Amplitude and EP and keep the td_ prefix
-- SELECT c.customer_uid AS c, a.uuid AS a
-- FROM src_elastic_path_data.cart c
-- JOIN src_amplitude_pokemoncenter.amplitude_pokemoncenter_prod a
-- ON c.customer_uid = a.user_id;
CREATE TABLE IF NOT EXISTS rr_tpci_stg.clicstream_events(
    time bigint,
    id varchar,
    source varchar,
    cp_email VARCHAR,
    customer_uid varchar,
    storecode varchar,
    cart_status varchar,
    shopping_cart_uid varchar,
    sku_code varchar,
    currency varchar,
    list_unit_price varchar,
    quantity varchar,
    item_type varchar,
    isfreegiftitem varchar,
    cart_item_creation_date varchar,
    shipping_option_code varchar,
    cp_first_name varchar,
    cp_last_name varchar,
    cp_phone varchar,
    billing_first_name varchar,
    billing_last_name varchar,
    billing_street_1 varchar,
    billing_city varchar,
    billing_sub_country varchar,
    billing_country varchar,
    payment_type_name varchar,
    promo_unit_price varchar,
    discount_amount varchar,
    amplitude_id varchar,
    device_id varchar,
    event_time varchar,
    server_upload_time varchar,
    client_event_time varchar,
    event_id varchar,
    session_id varchar,
    event_type varchar,
    library varchar,
    platform varchar,
    os_name varchar,
    os_version varchar,
    device_type varchar,
    country varchar,
    city varchar,
    uuid varchar,
    td_referrer varchar,
    td_path varchar,
    td_host varchar,
    td_ip varchar,
    td_color varchar,
    td_global_id varchar,
    td_language varchar,
    td_platform varchar,
    td_user_agent varchar,
    td_url varchar,
    td_description varchar,
    td_title varchar,
    td_viewport varchar,
    td_screen varchar,
    td_client_id varchar,
    td_global_id varchar,
    td_ip varchar,
    scarlet_violet_fan varchar,
    video_game_fan varchar,
    unite_fan varchar,
    animation_fan varchar,
    op_pe_fan varchar,
    tcg_fa varchar
) with (
    bucketed_on = array ['id'],
    bucket_count = 512
);
insert into rr_tpci_stg.clicstream_events
select time as time,
    event_time as event_time,
    cart_item_creation_date_unixtime as cart_item_creation_date_unixtime,
    cart_item_last_modified_date_unixtime as cart_item_last_modified_date_unixtime,
    shopping_cart_last_modified_date_unixtime as shopping_cart_last_modified_date_unixtime,
    id as id,
    source as source,
    cp_email as cp_email,
    email_address as email_address,
    customer_uid as customer_uid,
    storecode as storecode,
    cart_status as cart_status,
    shopping_cart_uid as shopping_cart_uid,
    sku_code as sku_code,
    currency as currency,
    list_unit_price as list_unit_price,
    quantity as quantity,
    item_type as item_type,
    isfreegiftitem as isfreegiftitem,
    cart_item_creation_date as cart_item_creation_date,
    shipping_option_code as shipping_option_code,
    cp_first_name as cp_first_name,
    cp_last_name as cp_last_name,
    cp_phone as cp_phone,
    phone as phone,
    billing_first_name as billing_first_name,
    billing_last_name as billing_last_name,
    billing_street_1 as billing_street_1,
    billing_city as billing_city,
    billing_sub_country as billing_sub_country,
    billing_country as billing_country,
    payment_type_name as payment_type_name,
    promo_unit_price as promo_unit_price,
    shipping_zip_postal_code,
    shipping_street_1,
    shipping_last_name,
    shipping_country,
    shipping_city,
    billing_zip_postal_code,
    billing_street_2,
    shipping_sub_country,
    shipping_first_name,
    shipping_street_2,
    sale_unit_price,
    rule_display_name,
    parent_item_uid,
    discount_amount as discount_amount,
    amplitude_id as amplitude_id,
    device_id as device_id,
    event_time as event_time,
    server_upload_time as server_upload_time,
    client_event_time as client_event_time,
    event_id as event_id,
    session_id as session_id,
    event_type as event_type,
    library as library,
    platform as platform,
    os_name as os_name,
    os_version as os_version,
    device_type as device_type,
    country as country,
    city as city,
    uuid as uuid,
    td_referrer as td_referrer,
    td_path as td_path,
    td_host as td_host,
    td_color as td_color,
    td_global_id as td_global_id,
    td_language as td_language,
    td_platform as td_platform,
    td_user_agent as td_user_agent,
    td_url as td_url,
    td_description as td_description,
    td_title as td_title,
    td_viewport as td_viewport,
    td_screen as td_screen,
    td_client_id as td_client_id,
    td_global_id as td_global_id,
    td_ip as td_ip,
    scarlet_violet_fan as scarlet_violet_fan,
    video_game_fan as video_game_fan,
    unite_fan as unite_fan,
    animation_fan as animation_fan,
    op_pe_fan as op_pe_fan,
    tcg_fan as tcg_fan
from(
        select time as time,
            TD_TIME_PARSE(cart_item_creation_date) as event_time,
            TD_TIME_PARSE(cart_item_last_modified_date) as cart_item_last_modified_date_unixtime,
            TD_TIME_PARSE(shopping_cart_last_modified_date) AS shopping_cart_last_modified_date_unixtime,
            to_base64url(
                xxhash64(
                    cast(
                        coalesce(customer_uid, '') || coalesce(shopping_cart_uid, '') || coalesce(sku_code, '') as varbinary
                    )
                )
            ) as id,
            'ELASTIC PATH' as source,
            cp_email as cp_email,
            CASE
                WHEN REGEXP_EXTRACT(LOWER(TRIM(cp_email)), '.+@(.+)', 1) in (
                    select exception_value
                    from src_snowflake.exception_list
                ) then null
                else LOWER(TRIM(cp_email))
            end AS email_address,
            storecode,
            cart_status,
            shopping_cart_uid,
            sku_code,
            currency,
            list_unit_price,
            quantity,
            customer_uid,
            item_type,
            isfreegiftitem,
            cart_item_creation_date,
            shipping_option_code,
            cp_first_name,
            cp_last_name,
            TRIM(cp_first_name || ' ' || cp_last_name) AS name,
            cp_phone as cp_phone,
            SUBSTRING(
                REGEXP_REPLACE(cp_phone, '\+1|\-|\.|\,|\(|\)|\#|\s|\+|^1'),
                1,
                20
            ) AS phone,
            billing_first_name,
            billing_last_name,
            billing_street_1,
            billing_city,
            billing_sub_country,
            billing_country,
            payment_type_name,
            promo_unit_price,
            shipping_zip_postal_code,
            shipping_street_1,
            shipping_last_name,
            shipping_country,
            shipping_city,
            billing_zip_postal_code,
            billing_street_2,
            shipping_sub_country,
            shipping_first_name,
            shipping_street_2,
            sale_unit_price,
            rule_display_name,
            parent_item_uid,
            discount_amount as discount_amount,
            cast(null as varchar) as amplitude_id,
            cast(null as varchar) as device_id,
            cast(null as varchar) as event_time,
            cast(null as varchar) as server_upload_time,
            cast(null as varchar) as client_event_time,
            cast(null as varchar) as event_id,
            cast(null as varchar) as session_id,
            cast(null as varchar) as event_type,
            cast(null as varchar) as library,
            cast(null as varchar) as platform,
            cast(null as varchar) as os_name,
            cast(null as varchar) as os_version,
            cast(null as varchar) as device_type,
            cast(null as varchar) as country,
            cast(null as varchar) as city,
            cast(null as varchar) as uuid,
            cast(null as varchar) as td_referrer,
            cast(null as varchar) as td_path,
            cast(null as varchar) as td_host,
            cast(null as varchar) as td_color,
            cast(null as varchar) as td_language,
            cast(null as varchar) as td_platform,
            cast(null as varchar) as td_user_agent,
            cast(null as varchar) as td_url,
            cast(null as varchar) as td_description,
            cast(null as varchar) as td_title,
            cast(null as varchar) as td_viewport,
            cast(null as varchar) as td_screen,
            cast(null as varchar) as td_client_id,
            cast(null as varchar) as td_global_id,
            cast(null as varchar) as td_ip,
            cast(null as varchar) as scarlet_violet_fan,
            cast(null as varchar) as video_game_fan,
            cast(null as varchar) as unite_fan,
            cast(null as varchar) as animation_fan,
            cast(null as varchar) as op_pe_fan,
            cast(null as varchar) as tcg_fan
        from src_elastic_path_data.cart
        where time > $ { td.last_results.last_session_time }
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary
                    )
                )
            ) not in (
                select id
                from rr_tpci_stg.identities
                where id is not null
            )
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary
                    )
                )
            ) is not null
        union ALL
        select time as time,
            TD_TIME_PARSE(event_time) as event_time,
            NULL as cart_item_last_modified_date_unixtime,
            NULL AS shopping_cart_last_modified_date_unixtime,
            TD_TIME_PARSE(client_event_time) AS client_event_time,
            TD_TIME_PARSE(client_upload_time) AS client_upload_time,
            to_base64url(
                xxhash64(
                    cast(
                        coalesce(cast(amplitude_id as varchar), '') || coalesce(cast(session_id as varchar), '') || coalesce(cast(uuid as varchar), '') as varbinary
                    )
                )
            ) as id,
            'AMPLITUDE' as source,
            cast(null as varchar) as cp_email,
            cast(null as varchar) as storecode,
            cast(null as varchar) as cart_status,
            cast(null as varchar) as shopping_cart_uid,
            cast(null as varchar) as sku_code,
            cast(null as varchar) as currency,
            cast(null as varchar) as list_unit_price,
            cast(null as varchar) as quantity,
            cast(null as varchar) as customer_uid,
            cast(null as varchar) as item_type,
            cast(null as varchar) as isfreegiftitem,
            cast(null as varchar) as cart_item_creation_date,
            cast(null as varchar) as shipping_option_code,
            cast(null as varchar) as cp_first_name,
            cast(null as varchar) as cp_last_name,
            cast(null as varchar) as cp_phone,
            cast(null as varchar) as phone_std,
            cast(null as varchar) as billing_first_name,
            cast(null as varchar) as billing_last_name,
            cast(null as varchar) as billing_street_1,
            cast(null as varchar) as billing_city,
            cast(null as varchar) as billing_sub_country,
            cast(null as varchar) as billing_country,
            cast(null as varchar) as payment_type_name,
            cast(null as varchar) as promo_unit_price,
            cast(null as varchar) as discount_amount,
            cast(amplitude_id as varchar) as amplitude_id,
            user_id as user_id,
            device_id as device_id,
            event_time as event_time,
            server_upload_time as server_upload_time,
            client_event_time as client_event_time,
            cast(event_id as varchar) as event_id,
            cast(session_id as varchar) as session_id,
            event_type,
            library,
            platform,
            os_name,
            os_version,
            device_type,
            device_family,
            country,
            city,
            uuid,
            cast(null as varchar) as td_referrer,
            processed_time,
            language,
            region,
            ip_address,
            cast(null as varchar) as td_path,
            cast(null as varchar) as td_host,
            cast(null as varchar) as td_ip,
            cast(null as varchar) as td_color,
            cast(null as varchar) as td_language,
            cast(null as varchar) as td_platform,
            cast(null as varchar) as td_user_agent,
            cast(null as varchar) as td_url,
            cast(null as varchar) as td_description,
            cast(null as varchar) as td_title,
            cast(null as varchar) as td_viewport,
            cast(null as varchar) as td_screen,
            cast(null as varchar) as td_client_id,
            cast(null as varchar) as td_global_id,
            cast(null as varchar) as td_ip,
            cast(null as varchar) as scarlet_violet_fan,
            cast(null as varchar) as video_game_fan,
            cast(null as varchar) as unite_fan,
            cast(null as varchar) as animation_fan,
            cast(null as varchar) as op_pe_fan,
            cast(null as varchar) as tcg_fa
        from src_amplitude_pokemoncenter.amplitude_pokemoncenter_prod
        where time > $ { td.last_results.last_session_time }
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary
                    )
                )
            ) not in (
                select id
                from rr_tpci_stg.identities
                where id is not null
            )
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary
                    )
                )
            ) is not null
        union ALL
        select time as time,
            time as min_insert_timestamp,
            td_time_parse(time) as min_update_timestamp,
            to_base64url(
                xxhash64(
                    cast(
                        coalesce(cast(td_client_id as varchar), '') || coalesce(cast(td_global_id as varchar), '') as varbinary
                    )
                )
            ) as id,
            'pcom' as source,
            cast(null as varchar) as cp_email,
            cast(null as varchar) as storecode,
            cast(null as varchar) as cart_status,
            cast(null as varchar) as shopping_cart_uid,
            cast(null as varchar) as sku_code,
            cast(null as varchar) as currency,
            cast(null as varchar) as list_unit_price,
            cast(null as varchar) as quantity,
            cast(null as varchar) as customer_uid,
            cast(null as varchar) as item_type,
            cast(null as varchar) as isfreegiftitem,
            cast(null as varchar) as cart_item_creation_date,
            cast(null as varchar) as shipping_option_code,
            cast(null as varchar) as cp_first_name,
            cast(null as varchar) as cp_last_name,
            cast(null as varchar) as cp_phone,
            cast(null as varchar) as phone_std,
            cast(null as varchar) as billing_first_name,
            cast(null as varchar) as billing_last_name,
            cast(null as varchar) as billing_street_1,
            cast(null as varchar) as billing_city,
            cast(null as varchar) as billing_sub_country,
            cast(null as varchar) as billing_country,
            cast(null as varchar) as payment_type_name,
            cast(null as varchar) as promo_unit_price,
            cast(null as varchar) as discount_amount,
            cast(null as varchar) as amplitude_id,
            cast(null as varchar) as device_id,
            cast(null as varchar) as event_time,
            cast(null as varchar) as server_upload_time,
            cast(null as varchar) as client_event_time,
            cast(null as varchar) as event_id,
            cast(null as varchar) as session_id,
            cast(null as varchar) as event_type,
            cast(null as varchar) as library,
            cast(null as varchar) as platform,
            cast(null as varchar) as os_name,
            cast(null as varchar) as os_version,
            cast(null as varchar) as device_type,
            cast(null as varchar) as country,
            cast(null as varchar) as city,
            cast(null as varchar) as uuid,
            td_referrer as td_referrer,
            td_path as td_path,
            td_host as td_host,
            td_ip as td_ip,
            td_color as td_color,
            td_language as td_language,
            td_platform as td_platform,
            td_user_agent as td_user_agent,
            td_url as td_url,
            td_description as td_description,
            td_title as td_title,
            td_viewport as td_viewport,
            td_screen as td_screen,
            td_client_id as td_client_id,
            td_global_id as td_global_id,
            td_ip as td_ip,
            CASE
                WHEN td_url like '%scarletviolet.pokemon.com%'
                OR td_url like '%teraincursiones%'
                OR td_url like '%raid-teracristal%'
                OR td_url like '%tera‑raids%'
                OR td_url like '%raids-teracristal%'
                OR td_url like '%tera-raid-battles%'
                OR td_url like '%pokemon-scarlet-and-pokemon-violet%'
                OR td_url like '%pokemon-ecarlate-et-pokemon-violet%'
                OR td_url like '%pokemon-scarlatto-e-pokemon-violetto%'
                OR td_url like '%pokemon-karmesin-und-pokemon-purpur%'
                OR td_url like '%pokemon-karmesin-und-pokemon-purpur%'
                OR td_url like '%pokemon-escarlata-y-pokemon-purpura%' THEN 'true'
                else 'false'
            end AS scarlet_violet_fan,
            CASE
                WHEN td_url like '%unite.%'
                OR td_url like '%scarletviolet.%'
                OR td_url like '%jeux-video%'
                OR td_url like '%video-games%'
                OR td_url like '%jeux-video%'
                OR td_url like '%videogiochi%'
                OR td_url like '%videospiele%'
                OR td_url like '%videojuegos%' THEN 'true'
                else 'false'
            end AS video_game_fan,
            CASE
                WHEN td_url like '%unite.%'
                OR td_url like '%-unite%'
                OR td_url like '%unite-%' THEN 'true'
                else 'false'
            end AS unite_fan,
            CASE
                WHEN td_url like '%episodes%'
                OR td_url like '%episodi%'
                OR td_url like '%folgen%'
                OR td_url like '%episodios%'
                OR td_url like '%watch.pokemon%'
                OR td_url like '%app/pokemon-tv%'
                OR td_url like '%animation%' THEN 'true'
                else 'false'
            end AS animation_fan,
            CASE
                WHEN td_url like '%play-pokemon%'
                OR td_url like '%events.pokemon.com%' THEN 'true'
                else 'false'
            end AS op_pe_fan,
            CASE
                WHEN td_url like '%tcg%'
                OR td_url like '%jcc-%'
                OR td_url like '%/gcc%'
                OR td_url like '%sammelkartenspiel%' THEN 'true'
                else 'false'
            end AS tcg_fan
        from src_amplitude_pokemoncenter.amplitude_pokemoncenter_prod
        where time > $ { td.last_results.last_session_time }
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary
                    )
                )
            ) not in (
                select id
                from rr_tpci_stg.identities
                where id is not null
            )
            and to_base64url(
                xxhash64(
                    cast(
                        coalesce(email, '') || coalesce(member_id, '') || coalesce(guid, '') as varbinary
                    )
                )
            ) is not null
    )