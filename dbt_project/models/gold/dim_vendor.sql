SELECT DISTINCT
    vendor_id,
    CASE vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc'
        ELSE 'Unknown'
    END as vendor_name
FROM {{ ref('trips_cleaned') }}