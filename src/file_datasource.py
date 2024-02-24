from csv import DictReader
from enum import Enum
from datetime import datetime
from marshmallow import Schema
from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema
from schema.parking_schema import ParkingEmptyCount
from domain.aggregated_data import AggregatedData
from domain.parking import Parking

class FileDatasource:
    class DataKeys(Enum):
        ACCELEROMETER = 0
        GPS = 1
        PARKING = 2
    def startReading(self):
        for reader in self.data_readers.values():
            reader.startReading()

    def stopReading(self):
        for reader in self.data_readers.values():
            reader.stopReading()
            
    def __init__(self, accel_file_path: str, gps_file_path: str, parking_file_path: str):
        self.data_readers = {
            self.DataKeys.ACCELEROMETER: ReaderSource(accel_file_path, AccelerometerSchema()),
            self.DataKeys.GPS: ReaderSource(gps_file_path, GpsSchema()),
            self.DataKeys.PARKING: ReaderSource(parking_file_path, ParkingEmptyCount())
        }

    def process(self, batch_size: int = 1):
        if any(not reader.csv_dict_reader for reader in self.data_readers.values()):
            raise Exception("CSV readers not initialized. Please call 'startReading' first.")
        
        results = []
        for _ in range(batch_size):
            data_bundle = {key: reader.process() for key, reader in self.data_readers.items()}
            timestamp = datetime.now()
            aggregated_data = AggregatedData(data_bundle[self.DataKeys.ACCELEROMETER], data_bundle[self.DataKeys.GPS], timestamp)
            parking_info = Parking(data_bundle[self.DataKeys.PARKING]["empty_count"], data_bundle[self.DataKeys.GPS])
            results.append((aggregated_data, parking_info))
        return results

class ReaderSource:
    def __init__(self, file_path, schema: Schema):
        self.file_path = file_path
        self.data_schema = schema
        self.file_handle = None
        self.csv_dict_reader = None

    def startReading(self):
        self.file_handle = open(self.file_path, 'r')
        self.csv_dict_reader = DictReader(self.file_handle)

    def process(self):
        row = next(self.csv_dict_reader, None)
        if row is None:
            self.restart()
            row = next(self.csv_dict_reader, None)
        return self.data_schema.load(row)

    def restart(self):
        self.file_handle.seek(0)
        self.csv_dict_reader = DictReader(self.file_handle)

    def stopReading(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            self.csv_dict_reader = None
