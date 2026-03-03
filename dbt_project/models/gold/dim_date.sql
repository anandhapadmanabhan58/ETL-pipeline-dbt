SELECT DISTINCT
    pickup_date,
    YEAR(pickup_date) as year,
    MONTH(pickup_date) as month,
    DAY(pickup_date) as day,
    DAYOFWEEK(pickup_date) as day_of_week,
    DAYNAME(pickup_date) as day_name,
    MONTHNAME(pickup_date) as month_name,
    CASE WHEN DAYOFWEEK(pickup_date) IN (1, 7) THEN true ELSE false END as is_weekend
FROM {{ ref('trips_cleaned') }}