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
    c.name as category,
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
LEFT JOIN {{ source('raw', 'film_category') }} fc ON f.film_id = fc.film_id
LEFT JOIN {{ source('raw', 'category') }} c ON fc.category_id = c.category_id 