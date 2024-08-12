-- project_id: 635838
-- project_name: staging
-- workflow_name: d_stage
-- session_id: 63446273
-- attempt_id: 543232847
-- task_name: +d_stage+stage_layer1+create_amp_abandoned_carts_native_connector
DROP TABLE IF EXISTS "amp_abandoned_carts_native_connector";

CREATE TABLE
  "amp_abandoned_carts_native_connector" AS
SELECT
  a.customer_uid,
  a.correlation_id,
  a.ip_address,
  case
    when a.country = 'GB' THEN 'UK'
    ELSE a.country
  END as ip_country,
  a.storefront,
  a.cart_id,
  a.product_sku,
  a.product_id,
  a.product_category,
  a.preorder_status,
  a.event_time,
  a.item_number_in_cart,
  a.sum_quantity,
  MAX(a.event_time) OVER (
    PARTITION BY
      cart_id
  ) as cart_last_touched_time,
  a.event_type,
  a.logged_in,
  case
    when preorder_status = 'available for pre order' then 'true'
    else 'false'
  end as preorder_flag,
  LOWER(b.three_day_low_inventory_prediction_flag) as three_day_low_inventory_prediction_flag,
  LOWER(b.hot_product) as hot_product,
  b.trailing_week_daily_average_sold,
  b.trailing_week_total_sold,
  b.style_description,
  b.sku_description,
  b.sku_web_description,
  b.product_url,
  b.product_image_url,
  b.preorder_end_date,
  b.quantity,
  b.is_buyable,
  b.authorized_to_sell
from
  src_amplitude_pokemoncenter.abandoned_carts a
  INNER JOIN tpci_stg.sflake_dim_product b ON a.product_sku = b.sku
  AND a.storefront = b.storefront_2_char_format
  AND a.event_type = 'Added to Cart'
ORDER BY
  a.product_sku,
  a.product_id