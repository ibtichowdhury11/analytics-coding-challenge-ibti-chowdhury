{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='append',
    tags=['staging_events', 'staging_events_full_refresh'],
    partition_by='event_date',
    order_by=['event_date', 'publisher_id', 'ad_unit_id'],
    pre_hook=[
        "{% if is_incremental() %} 
            ALTER TABLE {{ this }} DELETE 
            WHERE event_date BETWEEN {{ var('current_date_minus_10_days') }} AND {{ var('current_date') }}
            SETTINGS mutations_sync = 2
         {% endif %}"
    ]
  )
}}


WITH base_events AS (
    SELECT 

        event_id,
        event_type,
        event_timestamp,
        publisher_id,
        site_domain,
        ad_unit_id,
        campaign_id,
        advertiser_id,
        device_type,
        country_code,
        browser,
        revenue_usd,
        bid_floor_usd,
        is_filled,
        _loaded_at

    FROM {{ source('raw', 'ad_events') }}
    WHERE 
      {% if is_incremental() %}
       toDate(event_timestamp) BETWEEN {{ var('current_date_minus_10_days') }} AND {{ var('current_date') }}
      {% else %}
        toDate(event_timestamp) >= '{{ var('event_start_date') }}'
      {% endif %}
), 

base_events_clean AS (
    SELECT 

        event_timestamp,
        DATE(event_timestamp) AS event_date,
        dateTrunc('month', toDate(event_timestamp)) AS event_month,
        event_id,
        LOWER(event_type) as event_type,
        publisher_id,
        site_domain,
        ad_unit_id,
        NULLIF(campaign_id, 0) as campaign_id,
        NULLIF(advertiser_id, 0) as advertiser_id,
        device_type,
        LOWER(country_code) as country_code,
        browser,
        CAST(revenue_usd AS Decimal64(6)) as revenue_usd,
        CAST(bid_floor_usd AS Decimal64(6)) as bid_floor_usd,
        CAST(coalesce(is_filled, 0) AS Int64) as is_filled,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _loaded_at DESC) as latest_event_rank

    FROM base_events
)

    SELECT 

        event_timestamp,
        event_date,
        event_month,
        event_id,
        event_type,
        publisher_id,
        site_domain,
        ad_unit_id,
        campaign_id,
        advertiser_id,
        device_type,
        country_code,
        browser,
        is_filled,
        revenue_usd,
        bid_floor_usd
        

FROM base_events_clean
    WHERE latest_event_rank = 1
    AND event_timestamp <= now() 