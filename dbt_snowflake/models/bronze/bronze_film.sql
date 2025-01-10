{{
    config(
        materialized='view'
    )
}}

SELECT 
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    l.name as language,
    f.rental_duration,
    f.rental_rate,
    f.length,
    f.replacement_cost,
    f.rating,
    f.special_features,
    f.last_update,
    CURRENT_TIMESTAMP() as processed_date
FROM {{ source('raw', 'film') }} f
LEFT JOIN {{ source('raw', 'language') }} l ON f.language_id = l.language_id 