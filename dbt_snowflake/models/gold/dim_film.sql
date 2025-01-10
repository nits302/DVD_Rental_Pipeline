{{
    config(
        materialized='table'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['film_id']) }} as film_key,
    film_id,
    title,
    description,
    release_year,
    language,
    rental_duration,
    rental_rate,
    length,
    replacement_cost,
    rating,
    special_features,
    effective_date,
    current_flag,
    updated_at
FROM {{ ref('silver_film') }} 