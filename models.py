from pydantic import BaseModel, Field, field_validator, ValidationInfo
from datetime import datetime
from typing import Optional


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
