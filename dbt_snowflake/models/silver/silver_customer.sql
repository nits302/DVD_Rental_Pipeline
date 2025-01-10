{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

SELECT 
    c.customer_id,
    c.store_id,
    c.first_name,
    c.last_name,
    c.email,
    a.address,
    a.address2,
    a.district,
    ci.city,
    co.country,
    a.postal_code,
    a.phone,
    c.activebool,
    c.create_date,
    c.last_update as effective_date,
    c.processed_date as updated_at,
    TRUE as current_flag,
    CASE 
        WHEN c.active = 1 THEN TRUE
        ELSE FALSE 
    END as is_active
FROM {{ ref('bronze_customer') }} c
LEFT JOIN {{ source('raw', 'address') }} a ON c.address_id = a.address_id
LEFT JOIN {{ source('raw', 'city') }} ci ON a.city_id = ci.city_id
LEFT JOIN {{ source('raw', 'country') }} co ON ci.country_id = co.country_id

{% if is_incremental() %}
    WHERE c.processed_date > (SELECT max(updated_at) FROM {{ this }})
{% endif %}