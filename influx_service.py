import argparse

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

    return parser.parse_args()

class DataChecker(object):
    def __init__(self, influx_db):
        self.influx_db = influx_db

    async def main(self):
        async for data in RuuviTagSensor.get_data_async():
            print(data)
            mac = data[0]
            payload = data[1]
            self.influx_db.write_ruuvi_record(mac, payload)

if __name__ == "__main__":
    args = parse_cli_args()

    db = influx_writer.RuuviInfluxWriter(
        args.influx_url, args.influx_org, args.influx_bucket)
    
    dc = DataChecker(db)

    #RuuviTagSensor.get_data(write_to_influxdb)
    asyncio.get_event_loop().run_until_complete(dc.main())
