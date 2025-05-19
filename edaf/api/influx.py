import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from loguru import logger
import math, re, numbers

def is_duration_string(value):
    if not isinstance(value, str):
        return False

    # Normalize microseconds symbol to 'us'
    value = value.replace("µs", "us")

    return re.match(r"^\s*[\d\.]+\s*(ns|us|ms|s|m|h)\s*$", value.strip()) is not None

def convert_to_ms(value):
    """
    Convert a duration string (e.g. '21.45ms', '3us') to float milliseconds.
    """
    if pd.isna(value):
        return None  # treat missing as None

    if isinstance(value, (int, float)):
        return float(value)  # assume already in ms

    if isinstance(value, str):
        value = value.replace("µs", "us")

        # Match value like '21.45ms', '3us', etc.
        match = re.match(r"([\d\.]+)\s*(ns|us|ms|s|m|h)", value.strip())
        if not match:
            raise ValueError(f"Invalid duration format: {value}")

        number, unit = match.groups()
        number = float(number)

        unit_multipliers = {
            "ns": 1e-6,
            "us": 1e-3,
            "ms": 1,
            "s": 1e3,
            "m": 60_000,
            "h": 3_600_000,
        }

        return number * unit_multipliers[unit]

    raise ValueError(f"Unsupported value type: {type(value)}")

class InfluxClient:
    def __init__(self, influx_db_address, token, bucket, org):
        self.bucket = bucket
        self.org = org
        self.influx_db_address = influx_db_address
        self.token = token
        self.client = InfluxDBClient(url=influx_db_address, token=token)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def push_dataframe(self, df, point_name, time_key):

        points = []

        for index, row in df.iterrows():

            point = Point(point_name)
            for col in df.columns:
                if col == time_key:
                    continue
                value = row[col]
                # Skip None, NaNs, nans
                if pd.isnull(value) or pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
                    continue

                if ('buf' in col.lower()) or ('rnti' in col.lower()) or ( col.lower() == "source" ):
                    point = point.field(col, str(value))
                else:
                    if is_duration_string(value):
                        # This is a duration, convert to ms float
                        value = convert_to_ms(value)

                    point = point.field(col, float(value))

            point = point.time(int(float(row[time_key]) * 1e9), WritePrecision.NS)
            points.append(point)

        if points:
            self.write_api.write(self.bucket, self.org, points)

    def push_dataframe_list(self, publish_list):

        points = []

        for idx in range(len(publish_list)):
            
            df = publish_list[idx]["df"]
            point_name = publish_list[idx]["point_name"]
            time_key = publish_list[idx]["time_key"]

            for index, row in df.iterrows():

                point = Point(point_name)
                for col in df.columns:
                    if col == time_key:
                        continue
                    value = row[col]
                    # Skip None, NaNs, nans
                    if pd.isnull(value) or pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
                        logger.warning(f"[influx client] Detected None, NaNs, or nans in {point_name}, {col}")
                        continue

                    if ('buf' in col.lower()) or ('rnti' in col.lower()) or ( col.lower() == "source" ):
                        point = point.field(col, str(value))
                    else:
                        if is_duration_string(value):
                            # This is a duration, convert to ms float
                            value = convert_to_ms(value)

                        # check if value is non numeric, skip it. 
                        if isinstance(value, numbers.Number):
                            point = point.field(col, float(value))
                        else:
                            logger.warning(f"[influx client] Non numeric value on a numeric field {point_name}, {col}: {value}")
                            continue

                point = point.time(int(float(row[time_key]) * 1e9), WritePrecision.NS)
                points.append(point)

        if points:
            self.write_api.write(self.bucket, self.org, points)

    def __del__(self):
        self.client.close()
