{{
    config(
        materialized='view'
    )
}}

SELECT 
    customer_id,
    store_id,
    first_name,
    last_name,
    email,
    address_id,
    activebool,
    create_date,
    last_update,
    active,
    CURRENT_TIMESTAMP() as processed_date
FROM {{ source('raw', 'customer') }}