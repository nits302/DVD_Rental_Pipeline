{{
    config(
        materialized='table'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,
    store_id,
    manager_first_name,
    manager_last_name,
    address,
    address2,
    district,
    city,
    country,
    postal_code,
    phone,
    effective_date,
    current_flag,
    updated_at
FROM {{ ref('silver_store') }} 