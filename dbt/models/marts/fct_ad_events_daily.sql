{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='append',
    partition_by='date',
    order_by=['date', 'site_domain', 'device_type'],
    pre_hook=[
        "{% if is_incremental() %} 
            ALTER TABLE {{ this }} DELETE 
            WHERE date >= subtractDays(today(), 10)
            SETTINGS mutations_sync = 2
         {% endif %}"
    ]
  )
}}

WITH events AS (
    SELECT 
        event_date AS date,
        event_month AS month,
        publisher_id,
        site_domain,
        ad_unit_id,
        campaign_id,
        advertiser_id,
        device_type,
        country_code,
        browser,
        count(*) AS total_ad_requests,
        sum(is_filled) AS total_filled_impressions,
        countIf(event_type = 'impression') AS impressions,
        countIf(event_type = 'viewable_impression') AS viewable_impressions,
        countIf(event_type = 'click') AS clicks,
        sum(revenue_usd) AS total_revenue_usd,
        sum(bid_floor_usd) AS total_bid_floor_usd
    FROM {{ ref('stg_ad_events') }}
    WHERE 
      {% if is_incremental() %}
        event_date >= subtractDays(today(), 10)
      {% else %}
        event_date >= '2026-02-10'
      {% endif %}
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

publishers_meta AS (
    SELECT 
        publisher_id, 
        publisher_name,
        publisher_category
    FROM {{ ref('dim_publishers') }}
)

SELECT 
    -- Stable unique key with 'unknown' for NULLs
    lower(hex(MD5(concat(
        toString(COALESCE(b.date, '1970-01-01')), 
        toString(COALESCE(b.publisher_id, 0)), 
        toString(COALESCE(b.ad_unit_id, 'unknown')), 
        toString(COALESCE(b.site_domain, 'unknown')),
        toString(COALESCE(b.device_type, 'unknown')),
        toString(COALESCE(b.browser, 'unknown')),
        toString(COALESCE(b.campaign_id, 0)),
        toString(COALESCE(b.advertiser_id, 0)),
        toString(COALESCE(b.country_code, 'unknown'))
    )))) AS unique_key,
    b.date,
    b.month,
    COALESCE(b.site_domain, 'unknown') AS site_domain,
    COALESCE(b.ad_unit_id, 'unknown') AS ad_unit_id,
    b.campaign_id,
    b.advertiser_id,
    COALESCE(b.device_type, 'unknown') AS device_type,
    COALESCE(b.country_code, 'unknown') AS country_code,
    COALESCE(b.browser, 'unknown') AS browser,
    p.publisher_name,
    p.publisher_category,
    CAST(sum(b.total_filled_impressions) AS Float64) / NULLIF(sum(b.total_ad_requests), 0) AS fill_rate,
    sum(b.total_filled_impressions) AS total_filled_impressions,
    sum(b.impressions) AS impressions,
    sum(b.viewable_impressions) AS viewable_impressions,
    sum(b.clicks) AS clicks,
    sum(b.total_revenue_usd) AS total_revenue_usd,
    sum(b.total_bid_floor_usd) AS total_bid_floor_usd
FROM events b
LEFT JOIN publishers_meta p ON b.publisher_id = p.publisher_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    