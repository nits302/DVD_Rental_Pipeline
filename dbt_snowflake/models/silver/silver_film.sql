{{
    config(
        materialized='incremental',
        unique_key='film_id'
    )
}}

SELECT 
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
    last_update as effective_date,
    processed_date as updated_at,
    TRUE as current_flag
FROM {{ ref('bronze_film') }}

{% if is_incremental() %}
    WHERE processed_date > (SELECT max(updated_at) FROM {{ this }})
{% endif %}