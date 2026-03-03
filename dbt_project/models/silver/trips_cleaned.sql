SELECT
    vendor_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance,
    ratecode_id,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    airport_fee,
    DATE(pickup_at) as pickup_date,
    DATEDIFF('minute', pickup_at, dropoff_at) as trip_duration_minutes
FROM {{ ref('stg_trips') }}
WHERE
    passenger_count BETWEEN 1 AND 9
    AND trip_distance > 0
    AND fare_amount >= 0
    AND dropoff_at > pickup_at