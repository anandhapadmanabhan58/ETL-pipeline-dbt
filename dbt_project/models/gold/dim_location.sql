SELECT DISTINCT
    pu_location_id as location_id,
    CASE pu_location_id
        WHEN 1 THEN 'EWR'
        WHEN 132 THEN 'JFK Airport'
        WHEN 138 THEN 'LaGuardia Airport'
        WHEN 161 THEN 'Midtown Center'
        WHEN 162 THEN 'Midtown East'
        WHEN 163 THEN 'Midtown North'
        WHEN 230 THEN 'Times Square'
        ELSE 'Other'
    END as zone_name,
    CASE pu_location_id
        WHEN 1 THEN 'New Jersey'
        WHEN 132 THEN 'Queens'
        WHEN 138 THEN 'Queens'
        ELSE 'Manhattan'
    END as borough
FROM {{ ref('trips_cleaned') }}

UNION

SELECT DISTINCT
    do_location_id as location_id,
    CASE do_location_id
        WHEN 1 THEN 'EWR'
        WHEN 132 THEN 'JFK Airport'
        WHEN 138 THEN 'LaGuardia Airport'
        WHEN 161 THEN 'Midtown Center'
        WHEN 162 THEN 'Midtown East'
        WHEN 163 THEN 'Midtown North'
        WHEN 230 THEN 'Times Square'
        ELSE 'Other'
    END as zone_name,
    CASE do_location_id
        WHEN 1 THEN 'New Jersey'
        WHEN 132 THEN 'Queens'
        WHEN 138 THEN 'Queens'
        ELSE 'Manhattan'
    END as borough
FROM {{ ref('trips_cleaned') }}