{{
    config(
        materialized='table'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['staff_id']) }} as staff_key,
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
    is_active,
    effective_date,
    current_flag,
    updated_at
FROM {{ ref('silver_staff') }} 