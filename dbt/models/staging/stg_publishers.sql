{{
  config(
    materialized='table',
    unique_key='publisher_id',
    partition_by='toYYYYMM(updated_at)',
    order_by=['publisher_id']
  )
}}

WITH publishers_base AS (

    SELECT 
        publisher_id,
        publisher_name,
        publisher_category,
        primary_domain,
        account_manager,
        country,
        created_at,
        updated_at
    FROM {{ source('raw', 'publishers') }}
    WHERE toDate(updated_at) >= '2023-01-01'

),

ranked_publishers AS (

    SELECT 
        publisher_id,
        publisher_name,
        CASE 
            WHEN publisher_category = 'mobile_gaming' THEN 'mobile gaming'
            ELSE publisher_category 
        END AS publisher_category,
        LOWER(primary_domain) AS primary_domain,
        account_manager,
        UPPER(country) AS country_code,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY publisher_id ORDER BY updated_at DESC) AS latest_rank
    FROM publishers_base

)

SELECT 
    publisher_id,
    publisher_name,
    publisher_category,
    primary_domain,
    account_manager,
    country_code,
    created_at,
    updated_at
FROM ranked_publishers
WHERE latest_rank = 1