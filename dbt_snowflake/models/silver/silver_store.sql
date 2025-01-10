{{
    config(
        materialized='incremental',
        unique_key='store_id'
    )
}}

WITH store_data AS (
    SELECT 
        store_id,
        first_name,
        last_name,
        address,
        address2,
        district,
        city,
        country,
        postal_code,
        phone,
        last_update as effective_date,
        processed_date as updated_at
    FROM {{ ref('bronze_store') }}
)

SELECT 
    COALESCE(store_id, 0) as store_id,
    first_name as manager_first_name,
    last_name as manager_last_name,
    address,
    address2,
    district,
    city,
    country,
    postal_code,
    phone,
    effective_date,
    updated_at,
    TRUE as current_flag
FROM store_data
WHERE store_id IS NOT NULL

{% if is_incremental() %}
    AND processed_date > (SELECT max(updated_at) FROM {{ this }})
{% endif %} 