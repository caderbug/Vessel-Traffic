import os
import os
import requests
import zipfile
import re
import datetime
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd
from dataclasses import dataclass
import time
import shutil
import csv
from collections import defaultdict
import io

file_list = os.listdir("compressed_data")

print(file_list)

@dataclass
class time_point:
    time: int = None
    lat: float = None
    lon: float = None
    
    def decompress_time(self, date: str):
        
        total_seconds = int(self.time)
        
        h = total_seconds // 3600  # Hours
        m = (total_seconds % 3600) // 60  # Minutes
        s = total_seconds % 60  # Seconds

        year, month, day = map(int, date.split(":"))
        
        # Create a datetime object manually
        dt = datetime.datetime(year, month, day, h, m, s)
    
        # Convert to epoch time
        epoch_time = int(dt.timestamp())
        # Convert the datetime object to seconds since epoch
        self.time = epoch_time
        # print(self.time)

class ship:
    def __init__(self):
        self.MMSI = None
        self.date = None
        self.IMO = None
        self.callsign = None
        self.length = None
        self.width = None
        self.draft = None
        self.cargo = None
        self.vessel_type = None
        self.name = None
        self.time_series = list()

    def add_ship(self,
                 MMSI: str,
                 date: str,
                 vesselType: int,
                 cargo: str,
                 ):
        
        self.MMSI = MMSI

        year = date[0:2]
        month = date[2:4]
        day = date[4:6]
        self.date = f"20{year}:{month}:{day}"
        
        self.cargo = cargo
        self.vessel_type = vesselType

    def add_point_data(self, 
                       time: int,
                       lat: float,
                       lon: float,
                        ):
        data_point = time_point(time=time, lat=lat, lon=lon)
        data_point.decompress_time(self.date)

        self.time_series.append(data_point)
    
    def to_compressed_csv(self):
        ship_info = [
            self.MMSI,
            self.date,
            self.cargo,
            self.vessel_type
        ]
        time_series_data = [
            f"{tp.time};{tp.lat};{tp.lon}"
            for tp in self.time_series
        ]
        
        time_series_str = "|".join(time_series_data)
        return ",".join(map(str, ship_info)) + f',{time_series_str}'
    
    def is_west(self) -> bool:

        lon_count = 0

        for coord in self.time_series:
            if coord.lon > -100.0:
                lon_count += 1
        
        if lon_count / len(self.time_series) > .5:
            return False

        return True

    def seperate_trips(self):
        pass


ship_info = defaultdict(ship)

file_num = 0
data_dir = "compressed_data/"
cond_dir = "condensed_data/"
west_flag = True

os.makedirs(cond_dir, exist_ok=True)

MMSI = None

for file in file_list:

    with open(os.path.join(data_dir, file), "r") as f:
        with open(os.path.join(cond_dir, file), "w") as t:
            t.write("MMSI,Date,Cargo,VesselType,TimeSeries\n")
        
        f.readline()
        print(file)
        while(True):
            data = f.readline()
            if(data == ""):
                break
            data = data.strip("\n").split(",")
            # print(data)
            
            [MMSI, date, IMO, callsign, length, width, draft, cargo, vesseltype, name, timeseries] = data
        
            if MMSI not in ship_info.keys():

                ship_info[MMSI] = ship()
            
                ship_info[MMSI].add_ship(
                        MMSI, date, vesseltype, cargo
                )

            timeseries = timeseries.split("|")

            for ts in timeseries:
                [t, lat, lon, sog, cog, heading] = ts.split(";")
                ship_info[MMSI].add_point_data(int(t), float(lat), float(lon))

            ship_info[MMSI].time_series.sort(key=lambda x : x.lon)
            west_flag = ship_info[MMSI].is_west()
            
            with open(os.path.join(cond_dir, file), "a") as t:
                if west_flag:
                    t.write(ship_info[MMSI].to_compressed_csv() + "\n")

            west_flag = True
    ship_info = {}

    print("Added:", file)
    # exit(0)
    #     file_num += 1
    
    # if(file_num == 2):
    #     break


