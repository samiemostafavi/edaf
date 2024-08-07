import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

class InfluxClient:
    def __init__(self, influx_db_address, token, bucket, org, point_name, fields = None, time_key = "send.timestamp"):
        self.point_name = point_name
        self.bucket = bucket
        self.org = org
        self.time_key = time_key
        self.fields = fields
        self.influx_db_address = influx_db_address
        self.token = token
        self.client = InfluxDBClient(url=influx_db_address, token=token)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def push_dataframe(self, df):
        for index, row in df.iterrows():
            point = Point(self.point_name)
            if self.fields:
                for f in fields:
                    point.field(f, row[f])
            else:
                for f in df.keys():
                    point.field(f, row[f])

            point.time(datetime.fromtimestamp(row[self.time_key]), WritePrecision.NS)
            #point.time(datetime.utcnow(), WritePrecision.NS)

            #point = Point("e2e_delay").field("value", row['e2e_delay']).time(datetime.utcnow(), WritePrecision.NS)
            self.write_api.write(self.bucket, self.org, point)

    def __del__(self):
        self.client.close()

class InfluxClientFULL:
    def __init__(self, influx_db_address, token, bucket, org, point_name, fields, time_key = "send.timestamp"):
        self.bucket = bucket
        self.org = org
        self.time_key = time_key
        self.fields = fields
        self.influx_db_address = influx_db_address
        self.token = token
        self.client = InfluxDBClient(url=influx_db_address, token=token)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def push_dataframe(self, df):
        for index, row in df.iterrows():
            for col in df.columns:
                if col in self.fields:
                    point = Point(col).field("value", row[col]).time(int(float(row[self.time_key]) * 1e9), WritePrecision.NS)
                    self.write_api.write(self.bucket, self.org, point)

    def __del__(self):
        self.client.close()
