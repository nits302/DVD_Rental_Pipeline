{{
    config(
        materialized='incremental',
        unique_key='staff_id'
    )
}}

WITH staff_data AS (
    SELECT 
        staff_id,
        first_name,
        last_name,
        email,
        address,
        address2,
        district,
        city,
        country,
        postal_code,
        phone,
        store_id,
        username,
        active,
        last_update as effective_date,
        processed_date as updated_at
    FROM {{ ref('bronze_staff') }}
)

SELECT 
    staff_id,
    first_name,
    last_name,
    email,
    address,
    address2,
    district,
    city,
    country,
    postal_code,
    phone,
    COALESCE(store_id, 0) as store_id,
    username,
    CASE 
        WHEN COALESCE(active, 0) = 1 THEN TRUE
        ELSE FALSE 
    END as is_active,
    effective_date,
    updated_at,
    TRUE as current_flag
FROM staff_data
WHERE staff_id IS NOT NULL

{% if is_incremental() %}
    AND processed_date > (SELECT max(updated_at) FROM {{ this }})
{% endif %} 