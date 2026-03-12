{{
  config(
    materialized='table',
    unique_key='publisher_id',
    order_by=['publisher_id']
  )
}}

SELECT 
    publisher_id,
    publisher_name,
    publisher_category,
    primary_domain,
    account_manager,
    country_code,
    created_at,
    updated_at
FROM {{ ref('stg_publishers') }}