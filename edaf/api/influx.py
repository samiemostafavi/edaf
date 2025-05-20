import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from loguru import logger
import math, re, numbers
from decimal import Decimal

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
    def __init__(self, influx_db_address, token, org):
        self.org = org
        self.influx_db_address = influx_db_address
        self.token = token
        self.client = InfluxDBClient(url=influx_db_address, token=token)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def create_bucket(self, bucket):
        buckets_api = self.client.buckets_api()

        bucket_names = [b.name for b in buckets_api.find_buckets().buckets]
        if bucket not in bucket_names:
            buckets_api.create_bucket(bucket_name=bucket, org=self.org, retention_rules=[])

    def push_dataframe_list(self, publish_list, bucket):

        points = []

        for idx in range(len(publish_list)):
            
            df = publish_list[idx]["df"]
            point_name = publish_list[idx]["point_name"]
            time_key = publish_list[idx]["time_key"]

            for index, row in df.iterrows():

                point = Point(point_name)
                for col in df.columns:
                    value = row[col]

                    # Skip None, NaNs, nans
                    if pd.isnull(value) or pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
                        logger.debug(f"[influx client] Detected None, NaNs, or nans in {point_name}, {col}")
                        continue

                    # check if we have an str or no
                    if ('buf' in col.lower()) or ('rnti' in col.lower()) or ( col.lower() == "source" ):
                        point = point.field(col, str(value))
                    else:
                        if is_duration_string(value):
                            # This is a duration, convert to ms float
                            value = convert_to_ms(value)

                        # check if value is non numeric, skip it. 
                        try:
                            numeric_value = float(value)
                            point = point.field(col, numeric_value)
                        except (ValueError, TypeError):
                            logger.warning(f"[influx client] Non numeric value on a numeric field {point_name}, {col}: {value}")
                            continue

                #ns_time = int(float(row[time_key]) * 1e9)
                ns_time = int(Decimal(str(row[time_key])) * Decimal('1e9'))
                point = point.time(ns_time, WritePrecision.NS)
                points.append(point)

        if points:
            self.write_api.write(bucket, self.org, points)

    def fetch_recent_data(self, bucket : str, duration_ms : int):
        # Ensure the duration is formatted as a float string with 's' suffix
        duration_str = f"{duration_ms}" + "ms"
        query = f'''
        from(bucket: "{bucket}")
            |> range(start: -{duration_str})
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> group(columns: ["_measurement"])
        '''
        tables = self.query_api.query(query, org=self.org)

        dfs_list = []
        for table in tables:
            if not table.records:
                continue

            measurement = table.records[0].get_measurement()
            records = [record.values for record in table.records]

            df = pd.DataFrame(records)
            df.rename(columns={"_time": "time"}, inplace=True)
            df.drop(columns=["result", "table", "_start", "_stop"], errors="ignore", inplace=True)

            # Convert all columns containing "buf" in their name to int
            for col in df.columns:
                if "buf" in col.lower():
                    try:
                        df[col] = df[col].astype(float)
                        df[col] = df[col].astype(int)
                    except Exception as e:
                        logger.warning(f"[influx client] Failed to convert column {col} to int: {e}")

            dfs_list.append(
                {
                    "df": df,
                    "point_name": measurement,
                    "time_key": "time"
                }
            )

        return dfs_list

    def __del__(self):
        self.client.close()
