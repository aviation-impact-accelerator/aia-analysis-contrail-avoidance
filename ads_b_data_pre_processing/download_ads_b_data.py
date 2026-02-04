"""Script to download a subset of the ads-b data from aws."""

from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
import polars as pl

if TYPE_CHECKING:
    from polars.type_aliases import PolarsDataType

ADS_B_CSV_SCHEMA: dict[str, PolarsDataType] = {
    "timestamp": pl.String,
    "source": pl.String,
    "callsign": pl.String,
    "icao_address": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "altitude_baro": pl.Int32,
    "altitude_gnss": pl.Int32,
    "on_ground": pl.Boolean,
    "heading": pl.Float32,
    "speed": pl.Int32,
    "vertical_rate": pl.Int32,
    "squawk": pl.String,
    "aircraft_type_icao": pl.String,
    "aircraft_type_iata": pl.String,
    "tail_number": pl.String,
    "aircraft_type_name": pl.String,
    "airline_iata": pl.String,
    "airline_name": pl.String,
    "flight_number": pl.String,
    "departure_airport_icao": pl.String,
    "departure_airport_iata": pl.String,
    "arrival_airport_icao": pl.String,
    "arrival_airport_iata": pl.String,
    "departure_scheduled_time": pl.String,
    "arrival_scheduled_time": pl.String,
    "takeoff_time": pl.String,
    "landing_time": pl.String,
    "arrival_utc_offset": pl.Int32,
    "departure_utc_offset": pl.Int32,
}

bucket_name = "aia-data-ads-b"
raw_data_path = "raw"


number_of_files_to_get = 500

s3_client = boto3.client("s3")
paginator = s3_client.get_paginator("list_objects_v2")
pages = paginator.paginate(
    Bucket=bucket_name, Prefix="raw", PaginationConfig={"MaxItems": number_of_files_to_get}
)

paths = [
    f"s3://{bucket_name}/{obj['Key']}"
    for page in pages
    if "Contents" in page
    for obj in page["Contents"]
    if obj["Key"].endswith(".gzip")
]

scans = [
    pl.scan_csv(
        path,
        schema=ADS_B_CSV_SCHEMA,
    )
    for path in paths
]

lf = pl.concat(scans, how="vertical_relaxed")

# remove unnecessary columns
columns_to_keep = [
    "timestamp",
    "icao_address",
    "latitude",
    "longitude",
    "altitude_baro",
    "altitude_gnss",
    "heading",
    "aircraft_type_icao",
    "aircraft_type_name",
    "airline_iata",
    "flight_number",
    "departure_airport_icao",
    "arrival_airport_icao",
]

lf = lf.select(columns_to_keep)

# Save as aprox 100 parquet files
lf.sink_parquet(pl.PartitionBy("ads_b/", max_rows_per_file=5000000))
print("ADS-B data saved.")
