from models import TaxiTrip


def validate_rows(df):
    valid_rows = []
    invalid_rows = []

    for row in df.itertuples():
        try:
            trip = TaxiTrip(
                vendor_id=row.VendorID,
                pickup_at=row.tpep_pickup_datetime,
                dropoff_at=row.tpep_dropoff_datetime,
                passenger_count=row.passenger_count,
                trip_distance=row.trip_distance,
                ratecode_id=row.RatecodeID,
                store_and_fwd_flag=row.store_and_fwd_flag,
                pu_location_id=row.PULocationID,
                do_location_id=row.DOLocationID,
                payment_type=row.payment_type,
                fare_amount=row.fare_amount,
                extra=row.extra,
                mta_tax=row.mta_tax,
                tip_amount=row.tip_amount,
                tolls_amount=row.tolls_amount,
                improvement_surcharge=row.improvement_surcharge,
                total_amount=row.total_amount,
                congestion_surcharge=row.congestion_surcharge,
                airport_fee=row.Airport_fee,
            )
            valid_rows.append(trip)
        except Exception as e:
            invalid_rows.append({"row": row, "error": str(e)})

    return valid_rows, invalid_rows
