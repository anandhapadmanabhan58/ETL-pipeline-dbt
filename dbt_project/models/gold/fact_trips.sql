SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_at']) }} as trip_id,
    vendor_id,
    pickup_date,
    pu_location_id,
    do_location_id,
    payment_type,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    total_amount
FROM {{ ref('trips_cleaned') }}