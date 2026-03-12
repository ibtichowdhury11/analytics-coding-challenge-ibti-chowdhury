{{
    config(
        materialized='table',
        unique_key='ad_unit_id',
        order_by=['publisher_id', 'ad_unit_id']
    )
}}

WITH ad_units_base AS (
    SELECT
        ad_unit_id,
        publisher_id,
        ad_unit_name,
        ad_format,
        ad_size,
        placement_type,
        is_active,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY ad_unit_id ORDER BY created_at DESC ) AS latest_rank
    FROM {{ source('raw', 'ad_units') }}
)

SELECT
    ad_unit_id,
    publisher_id,
    ad_unit_name,
    ad_format,
    ad_size,
    placement_type,
    CAST(is_active AS UInt8) AS is_active,
    toDate(created_at) AS created_at
FROM ad_units_base
WHERE latest_rank = 1