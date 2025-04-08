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


class time_point:

    __slots__ = ['time', 'lat', 'lon']

    # def time_point(self):

    #     self.time: int = None
    #     self.lat: float = None
    #     self.lon: float = None
    
    def __init__(self, time: int, lat: float, lon: float):
        self.time: int = time
        self.lat: float = lat
        self.lon: float = lon        

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

    __slots__ = ('MMSI', 'date', 'cargo', 'vessel_type', 'time_series')

    def __init__(self):
        self.MMSI = None
        self.date = None
        self.cargo = None
        self.vessel_type = None
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
file_list = os.listdir("condensed_data/")
file_num = 0
data_dir = "compressed_data/"
cond_dir = "condensed_data/"
joined_m_dir = "joined_monthly/"
west_flag = True
monthly_file = "2019_01"

with open(os.path.join(cond_dir, "2019_01.csv"), "w") as t:
            t.write("MMSI,Date,Cargo,VesselType,TimeSeries\n")

os.makedirs(cond_dir, exist_ok=True)

month_list = [f"2019_{x:02}" for x in range(1, 13)] + [f"2020_{x:02}" for x in range(1, 13)]

# print(month_list)
# exit(0)
MMSI = None
for month in month_list:
    ship_info = {}

    curr_range = month
    curr_file = f"{month}.csv"

    with open(os.path.join(cond_dir, curr_file), "w") as t:
            t.write("MMSI,Date,Cargo,VesselType,TimeSeries\n")

    for file in file_list:
        
        if curr_range in file:
            
            print(file)
            with open(os.path.join(cond_dir, file), "r") as f:
                
                f.readline()
                while(True):
                    
                    data = f.readline()
                    if(data == ""):
                        break
                    data = data.strip("\n").split(",")
                    # print(data)
                    
                    [MMSI, date, cargo, vesseltype, timeseries] = data
                
                    if MMSI not in ship_info.keys():

                        ship_info[MMSI] = ship()
                    
                        ship_info[MMSI].add_ship(
                                MMSI, date, vesseltype, cargo
                        )

                    timeseries = timeseries.split("|")

                    for ts in timeseries:
                        [t, lat, lon] = ts.split(";")
                        ship_info[MMSI].add_point_data(int(t), float(lat), float(lon))


    with open(os.path.join(joined_m_dir, curr_file), "a") as t:
        for s in ship_info.values():
            t.write(s.to_compressed_csv() + "\n")

git ad
    print("Added:", file)
    # exit(0)
    #     file_num += 1
    
    # if(file_num == 2):
    #     break


