{{
    config(
        materialized='table',
        unique_key='campaign_id'
        order_by=['advertiser_id', 'campaign_id']
    )
}}

WITH campaign_base AS (
    SELECT
        campaign_id,
        campaign_name,
        advertiser_id,
        advertiser_name,
        campaign_start_date,
        campaign_end_date,
        campaign_budget_usd,
        campaign_status,
        targeting_device_types,
        targeting_countries,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id 
            ORDER BY created_at DESC
        ) AS latest_rank
    FROM {{ source('raw', 'campaigns') }}
)

SELECT 
    campaign_id,
    campaign_name,
    advertiser_id,
    advertiser_name,
    toDate(campaign_start_date) AS campaign_start_date,
    toDate(campaign_end_date) AS campaign_end_date,
    CAST(campaign_budget_usd AS Decimal64(2)) AS campaign_budget_usd,
    campaign_status,
    targeting_device_types,
    targeting_countries,
    created_at
FROM campaign_base
WHERE latest_rank = 1