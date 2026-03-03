import pandas as pd
import duckdb
from pydantic import BaseModel, Field, field_validator, ValidationInfo
from datetime import datetime
from typing import Optional
import pandas as pd
import os

conn = duckdb.connect()

df = conn.execute("""
    SELECT *
    FROM 'yellow_tripdata_2024-01.parquet'
    LIMIT 100
""").df()


class TaxiTrip(BaseModel):
    vendor_id: int
    pickup_at: datetime
    dropoff_at: datetime
    passenger_count: int = Field(ge=1, le=9)
    trip_distance: float = Field(gt=0)
    ratecode_id: Optional[int]
    store_and_fwd_flag: Optional[str]
    pu_location_id: int
    do_location_id: int
    payment_type: int
    fare_amount: float = Field(ge=0)
    extra: float
    mta_tax: float
    tip_amount: float = Field(ge=0)
    tolls_amount: float = Field(ge=0)
    improvement_surcharge: float
    total_amount: float
    congestion_surcharge: Optional[float]
    airport_fee: Optional[float]

    @field_validator('dropoff_at')
    @classmethod
    def dropoff_must_beafter_pickup(cls, dropoff, values: ValidationInfo):
        pickup = values.data.get('picked_at')
        if pickup and dropoff <= pickup:
            raise ValueError('dropoff must be after pickup')
        return dropoff


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

print(f"Valid rows: {len(valid_rows)}")
print(f"inValid rows: {len(invalid_rows)}")
print(invalid_rows[0]['error'])

print(df['passenger_count'].value_counts())

for i, invalid in enumerate(invalid_rows):
    print(f"Row {i+1}: {invalid['error']}")
    print("---")

valid_df = pd.DataFrame([row.model_dump() for row in valid_rows])
# normal save
# valid_df.to_parquet('gcs/alidated_trips.parquet', index=False)

valid_df['pickup_date'] = valid_df['pickup_at'].dt.date
# partition
for date, group in valid_df.groupby('pickup_date'):
    folder = f'gcs/{str(date)[:4]}/{str(date)[5:7]}'
    os.makedirs(folder, exist_ok=True)
    group.drop(columns=['pickup_date']).to_parquet(
        f'{folder}/{date}.parquet', index=False)
    print(f"Saved {len(group)} rows → {folder}/{date}.parquet")

print(f"Saved {len(valid_df)} rows to validated_trips.parquet")
