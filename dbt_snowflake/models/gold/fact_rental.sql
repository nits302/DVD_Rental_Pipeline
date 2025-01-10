{{
    config(
        materialized='incremental',
        unique_key='rental_key'
    )
}}

WITH rental_with_inventory AS (
    SELECT 
        r.*,
        COALESCE(i.film_id, 0) as film_id,
        COALESCE(i.store_id, 0) as store_id
    FROM {{ source('raw', 'rental') }} r
    LEFT JOIN {{ source('raw', 'inventory') }} i ON r.inventory_id = i.inventory_id
    WHERE i.inventory_id IS NOT NULL  -- Filter out invalid inventory records
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['r.rental_id']) }} as rental_key,
    r.rental_id,
    COALESCE(dc.customer_key, '0') as customer_key,
    COALESCE(df.film_key, '0') as film_key,
    COALESCE(ds.store_key, '0') as store_key,
    COALESCE(dst.staff_key, '0') as staff_key,
    COALESCE(dd_rental.date_key, '0') as rental_date_key,
    COALESCE(dd_return.date_key, '0') as return_date_key,
    r.rental_date,
    r.return_date,
    COALESCE(p.amount, 0) as payment_amount,
    COALESCE(df.rental_duration, 0) as rental_duration,
    CURRENT_TIMESTAMP as created_at
FROM rental_with_inventory r
LEFT JOIN {{ ref('dim_customer') }} dc ON r.customer_id = dc.customer_id
LEFT JOIN {{ ref('dim_film') }} df ON r.film_id = df.film_id
LEFT JOIN {{ ref('dim_store') }} ds ON r.store_id = ds.store_id
LEFT JOIN {{ ref('dim_staff') }} dst ON r.staff_id = dst.staff_id
LEFT JOIN {{ ref('dim_date') }} dd_rental ON DATE(r.rental_date) = dd_rental.full_date
LEFT JOIN {{ ref('dim_date') }} dd_return ON DATE(r.return_date) = dd_return.full_date
LEFT JOIN {{ source('raw', 'payment') }} p ON r.rental_id = p.rental_id
WHERE dc.customer_key IS NOT NULL  -- Ensure we have valid customer
  AND df.film_key IS NOT NULL     -- Ensure we have valid film
  AND ds.store_key IS NOT NULL    -- Ensure we have valid store
  AND dst.staff_key IS NOT NULL   -- Ensure we have valid staff

{% if is_incremental() %}
    AND r.rental_date > (SELECT max(rental_date) FROM {{ this }})
{% endif %}