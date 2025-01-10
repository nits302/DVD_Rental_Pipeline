{{
    config(
        materialized='view'
    )
}}

SELECT 
    s.store_id,
    st.first_name,
    st.last_name,
    a.address,
    a.address2,
    a.district,   
    c.city,
    co.country,
    a.postal_code,
    a.phone,
    s.last_update,
    CURRENT_TIMESTAMP() as processed_date
FROM {{ source('raw', 'store') }} s
LEFT JOIN {{ source('raw', 'staff') }} st ON s.manager_staff_id = st.staff_id
LEFT JOIN {{ source('raw', 'address') }} a ON s.address_id = a.address_id
LEFT JOIN {{ source('raw', 'city') }} c ON a.city_id = c.city_id
LEFT JOIN {{ source('raw', 'country') }} co ON c.country_id = co.country_id