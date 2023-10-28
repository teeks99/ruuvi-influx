import os
import influxdb_client
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

class RuuviInfluxWriter(object):
    def __init__(self, url, org, bucket, token=None):
        if not token:
            token = self.find_token()

        self.org = org
        self.bucket = bucket

        self.write_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=org)
        
        self.write_api = self.write_client.write_api(write_options=SYNCHRONOUS)

    def find_token(self):
        if "INFLUXDB_TOKEN" in os.environ:
            token = os.environ.get("INFLUXDB_TOKEN")
        elif os.path.isfile("influxdb_token.txt"):
            with open("influxdb_token.txt", "r") as token_f:
                token = token_f.read()
        return token

    def write_ruuvi_record(self, mac, payload):
        """
        Convert data into RuuviCollector naming scheme and scale

        returns:
            Object to be written to InfluxDB
        """
        dataFormat = payload["data_format"] if ("data_format" in payload) else None
        fields = {}
        fields["temperature"] = payload["temperature"] if ("temperature" in payload) else None
        fields["humidity"] = payload["humidity"] if ("humidity" in payload) else None
        fields["pressure"] = payload["pressure"] if ("pressure" in payload) else None
        fields["accelerationX"] = payload["acceleration_x"] if ("acceleration_x" in payload) else None
        fields["accelerationY"] = payload["acceleration_y"] if ("acceleration_y" in payload) else None
        fields["accelerationZ"] = payload["acceleration_z"] if ("acceleration_z" in payload) else None
        fields["batteryVoltage"] = payload["battery"] / 1000.0 if ("battery" in payload) else None
        fields["txPower"] = payload["tx_power"] if ("tx_power" in payload) else None
        fields["movementCounter"] = payload["movement_counter"] if ("movement_counter" in payload) else None
        fields["measurementSequenceNumber"] = (
            payload["measurement_sequence_number"] if ("measurement_sequence_number" in payload) else None
        )
        fields["tagID"] = payload["tagID"] if ("tagID" in payload) else None
        fields["rssi"] = payload["rssi"] if ("rssi" in payload) else None
        json_body = [
            {"measurement": "ruuvi_measurements", "tags": {"mac": mac, "dataFormat": dataFormat}, "fields": fields}
        ]

        self.write_api.write(bucket=self.bucket, org=self.org, record=json_body)