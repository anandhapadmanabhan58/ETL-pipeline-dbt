import duckdb
import pandas as pd
import os
from validate import validate_rows

conn = duckdb.connect()

df = conn.execute("""
    SELECT *
    FROM 'yellow_tripdata_2024-01.parquet'
    LIMIT 100
""").df()

valid_rows, invalid_rows = validate_rows(df)

print(f"Valid rows:   {len(valid_rows)}")
print(f"Invalid rows: {len(invalid_rows)}")

valid_df = pd.DataFrame([row.model_dump() for row in valid_rows])
valid_df['pickup_date'] = valid_df['pickup_at'].dt.date

for date, group in valid_df.groupby('pickup_date'):
    folder = f'gcs/{str(date)[:4]}/{str(date)[5:7]}'
    os.makedirs(folder, exist_ok=True)
    group.drop(columns=['pickup_date']).to_parquet(
        f'{folder}/{date}.parquet', index=False)
    print(f"Saved {len(group)} rows → {folder}/{date}.parquet")
