{{
    config(
        materialized='table'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
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
    is_active,
    create_date,
    effective_date,
    current_flag,
    updated_at
FROM {{ ref('silver_customer') }} 