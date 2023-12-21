import argparse
import datetime
import socket
import urllib.request

import os
os.environ["RUUVI_BLE_ADAPTER"] = "bleak"

from ruuvitag_sensor.ruuvi import RuuviTagSensor
import asyncio

import influx_writer

db = None

def parse_cli_args():
    parser = argparse.ArgumentParser(description=
        'Capture Ruuvi sensor data and send to influxdb')
    parser.add_argument(
        '--influx-org', default="ruuvi",
        help='Influxdb org')
    parser.add_argument(
        '--influx-url', default="http://localhost:8086",
        help="Influxdb URL")
    parser.add_argument(
        '--influx-bucket', default="ruuvi",
        help="Influxdb bucket to store records in")
    parser.add_argument(
        "--max-interval-sec", default="0",
        help="If this is set, only updates that have changed will be stored " +
        "or a minimum of this many seconds")
    parser.add_argument(
        "--check-url", default="",
        help="URL to notify each time that a packet is sent.")
    parser.add_argument(
        "--allow-mac", "-m", action="append", default=[],
        help="MAC address to support, if none specified support all. " +
        "Use multiple times for multiple MACs")

    return parser.parse_args()

class DataChecker(object):
    def __init__(
            self, influx_db, max_interval_sec=0, allow_macs=[], check_url=None):
        self.influx_db = influx_db
        self.allow_macs = allow_macs

        self.max_interval = datetime.timedelta(seconds=max_interval_sec)
        self.last_stored = {}

        self.check_url = check_url

    async def main(self):
        async for data in RuuviTagSensor.get_data_async():
            print(f"{datetime.datetime.now().isoformat()} - {data}")
            self.mac = data[0]
            self.payload = data[1]

            if self.check_for_send():
                self.influx_db.write_ruuvi_record(self.mac, self.payload)
                self.notify_url()

    def check_for_send(self):
        if not self.valid_mac():
            return False

        self.trimmed_payload = self.payload.copy()
        del self.trimmed_payload["measurement_sequence_number"]

        if not self.max_interval:
            self.store()
            return True

        if self.ready():
            self.store()
            return True

        return False

    def store(self):
        self.last_stored[self.mac] = {
            "payload": self.trimmed_payload, 
            "last_time": datetime.datetime.utcnow()
        }

    def ready(self):
        if not self.mac in self.last_stored:
            return True
        
        if self.trimmed_payload != self.last_stored[self.mac]["payload"]:
            return True

        last_time = self.last_stored[self.mac]["last_time"]
        if datetime.datetime.utcnow() - last_time > self.max_interval:
            return True

        return False

    def valid_mac(self):
        if not self.allow_macs:
            return True

        return self.mac in self.allow_macs

    def notify_url(self):
        if self.check_url:
            try:
                urllib.request.urlopen(self.check_url, timeout=10)
            except socket.error as e:
                print(f"Check URL call failed: {e}")


if __name__ == "__main__":
    args = parse_cli_args()

    db = influx_writer.RuuviInfluxWriter(
        args.influx_url, args.influx_org, args.influx_bucket)
    
    dc = DataChecker(
        db, int(args.max_interval_sec), args.allow_mac, args.check_url)

    #RuuviTagSensor.get_data(write_to_influxdb)
    asyncio.get_event_loop().run_until_complete(dc.main())
