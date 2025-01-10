{{
    config(
        materialized='view'
    )
}}

SELECT 
    s.staff_id,
    s.first_name,
    s.last_name,
    s.email,
    a.address,
    a.address2,
    a.district,
    c.city,
    co.country,
    a.postal_code,
    a.phone,
    s.store_id,
    CAST(CASE 
        WHEN LOWER(s.active) = 'true' THEN 1
        WHEN LOWER(s.active) = 'false' THEN 0
        ELSE COALESCE(CAST(s.active AS NUMBER(38,0)), 0)
    END AS NUMBER(38,0)) as active,
    s.username,
    s.last_update,
    CURRENT_TIMESTAMP() as processed_date
FROM {{ source('raw', 'staff') }} s
LEFT JOIN {{ source('raw', 'address') }} a ON s.address_id = a.address_id
LEFT JOIN {{ source('raw', 'city') }} c ON a.city_id = c.city_id
LEFT JOIN {{ source('raw', 'country') }} co ON c.country_id = co.country_id