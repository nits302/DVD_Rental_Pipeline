{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    date_day as full_date,
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    TO_CHAR(date_day, 'Month') as month_name,
    EXTRACT(WEEK FROM date_day) as week,
    EXTRACT(DAY FROM date_day) as day,
    EXTRACT(DOW FROM date_day) as day_of_week,
    TO_CHAR(date_day, 'Day') as day_name,
    CASE 
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END as is_weekend,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM date_spine 