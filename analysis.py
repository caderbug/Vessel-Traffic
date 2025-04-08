
# Lat/Long in D.D
# Port of Los Angeles: 33.73 / 118.26
# Port of Long Beach: 33.7583° N / 118.195°
# Port of Oakland: 37.7955° N, 122.2846°

import pandas as pd
import numpy as np
from datasets import load_dataset
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# List to store filenames
file_list_19 = []

# Starting date and ending date for 2019
start_date = datetime(2019, 1, 1)
end_date = datetime(2019, 12, 31)

# Generate filenames for each day
current_date = start_date
while current_date <= end_date:
    filename = current_date.strftime("AIS_%Y_%m_%d.csv")  # Example: AIS_2019_01_01.csv
    file_list_19.append(filename)
    current_date += timedelta(days=1)
#ds2 = load_dataset("andy135/monthly_compressed", data_files=file_list_19)

file_list_20 = []
start_date = datetime(2020, 1, 1)
end_date = datetime(2020, 12, 31)

# Generate filenames for each day
current_date = start_date
while current_date <= end_date:
    filename = current_date.strftime("AIS_%Y_%m_%d.csv")  # Example: AIS_2019_01_01.csv
    file_list_20.append(filename)
    current_date += timedelta(days=1)
#ds2 = load_dataset("andy135/monthly_compressed", data_files=file_list_20)


overall_time_19 = 0
num_ships_19 = 0
overall_time_20 = 0
num_ships_20 = 0

df1 = pd.read_csv('AIS_2019_01_01.csv')
MMSI_data_2019 = df1['MMSI']
Time_data_2019 = df1['TimeSeries']
MMSI_data_2019 = df1.iloc[:, 0]
Time_data_2019 = df1.iloc[:, 4]

df2 = pd.read_csv('AIS_2020_01_01.csv')
MMSI_data_2020 = df2['MMSI']
Time_data_2020 = df2['TimeSeries']
MMSI_data_2020 = df2.iloc[:, 0]
Time_data_2020 = df2.iloc[:, 4]

array_Time_2019 = []
array_Time_2020 = []
data_2019 = {} #Key: MMSI, Value: Time Series
data_2020 = {}
# Get time series data
# append any overflowing values for specific MMSI
g = 0
result_array_19 = []
result_array_20 = []
previous_value_19 = None
previous_value_20 = None

for index, value in enumerate(Time_data_2019):
    if pd.isna(value):  # If current value is NaN
        newi = index - 1  # Previous index

        # Safely check if previous index is valid
        if newi >= 0 and newi < len(array_Time_2019):
            try:
                # Safely access MMSI_data_2019
                if index < len(MMSI_data_2019):
                    c = array_Time_2019[newi] + MMSI_data_2019.iloc[index]
                    result_array_19.append(c)  # Append the concatenated value
                else:
                    print(f"Index {index} is out of range for MMSI_data_2019.")
            except Exception as e:
                print(f"Error calculating c at index {index}: {e}")
        else:
            print(f"Invalid previous index: {newi}. Skipping.")
    else:
        # Append current value to the result array and update previous_value
        if previous_value_19 is not None:
            concatenated_value = previous_value_19 + value  # Combine previous and current value
            result_array_19.append(concatenated_value)
        else:
            result_array_19.append(value)  # No previous value, append as is

        previous_value_19 = value  # Update previous_value

#2020

for index, value in enumerate(Time_data_2020):
    if pd.isna(value):  # If current value is NaN
        newi = index - 1  # Previous index

        # Safely check if previous index is valid
        if newi >= 0 and newi < len(array_Time_2020):
            try:
                # Safely access MMSI_data_2019
                if index < len(MMSI_data_2020):
                    c = array_Time_2020[newi] + MMSI_data_2020.iloc[index]
                    result_array_20.append(c)  # Append the concatenated value
                else:
                    print(f"Index {index} is out of range for MMSI_data_2019.")
            except Exception as e:
                print(f"Error calculating c at index {index}: {e}")
        else:
            print(f"Invalid previous index: {newi}. Skipping.")
    else:
        # Append current value to the result array and update previous_value
        if previous_value_20 is not None:
            concatenated_value = previous_value_20 + value  # Combine previous and current value
            result_array_20.append(concatenated_value)
        else:
            result_array_20.append(value)  # No previous value, append as is

        previous_value_20 = value  # Update previous_value



l = 0
array_MMSI_2019 = []
array_MMSI_2020 = []

# read MMSI data from both years

for value in MMSI_data_2019:
  if value.isdigit():
      num_ships_19 = num_ships_19 + 1
      array_MMSI_2019.append(int(value))
array_MMSI_2019 = np.array(array_MMSI_2019)

k = 0

for value in MMSI_data_2020:
  if value.isdigit():
      num_ships_20 = num_ships_20 + 1
      array_MMSI_2020.append(int(value))
array_MMSI_2020 = np.array(array_MMSI_2020)

n = 0
j = 0
total_time = 0
epoch = 0
q = 1
while(q):
    ship = result_array_19[n].split('|')
    if j < len(ship):
        data = ship[j].split(';')

        if len(data) == 3:
            (time,lat,long) = data
        #print(len(ship))
        #print(ship)
    #print("time:",time, "lat:", float(lat), "long:", long)

        if(float(lat) < 33.6811 and float(long) < -118.299) and (float(lat) > 33.7780 and float(long) < -118.2999) and (float(lat) < 33.6821 and float(long) > -118.1377) and (float(lat) < 33.7783 and float(long) > -118.1377):
            print("out range")
        else:
            epoch = float(time) % 86400
            print(epoch)
            total_time = epoch - total_time
            total_time = total_time % 3600      # total hours of all ships at port
        j = j + 1
    else:
        n = n + 1
    if n == len(result_array_19):
        q = 0

overall_time_19 = overall_time_19 + total_time
print("total time:",overall_time_19)
n = 0
j = 0
total_time = 0
epoch = 0
q = 1
while(q):
    ship = result_array_20[n].split('|')
    if j < len(ship):
        data = ship[j].split(';')

        if len(data) == 3:
            (time,lat,long) = data
        #print(len(ship))
        #print(ship)
    #print("time:",time, "lat:", float(lat), "long:", long)

        if(float(lat) < 33.6811 and float(long) < -118.299) and (float(lat) > 33.7780 and float(long) < -118.2999) and (float(lat) < 33.6821 and float(long) > -118.1377) and (float(lat) < 33.7783 and float(long) > -118.1377):
            print("out range")
        else:
            epoch = float(time) % 86400
            print(epoch)
            total_time = epoch - total_time
            total_time = total_time % 3600      # total hours of all ships at port
        j = j + 1
    else:
        n = n + 1
    if n == len(result_array_20):
        q = 0

overall_time_20 = overall_time_20 + total_time

new_time = overall_time_19 % num_ships_19
seconds_elapsed = np.arange(0, 3600 * 24 * 30 * 12, 3600 * 24 * 30)
months = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Plot
plt.figure(figsize=(10, 6))
plt.plot(seconds_elapsed, new_time, marker='o', linestyle='-', color='b')

# Customize the plot
plt.title("Graph with Unrelated Time Variable and Monthly Data")
plt.xlabel("Unrelated Time Variable")
plt.ylabel("Monthly Values")
plt.xticks(new_time, months, rotation=45)  # Replace x-axis ticks with months
plt.grid(True)

# Show Plot
plt.tight_layout()
plt.show()

new_time = overall_time_20 % num_ships_20
seconds_elapsed = np.arange(0, 3600 * 24 * 30 * 12, 3600 * 24 * 30)
months = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Plot
plt.figure(figsize=(10, 6))
plt.plot(seconds_elapsed, new_time, marker='o', linestyle='-', color='b')

# Customize the plot
plt.title("Graph with Unrelated Time Variable and Monthly Data")
plt.xlabel("Unrelated Time Variable")
plt.ylabel("Monthly Values")
plt.xticks(new_time, months, rotation=45)  # Replace x-axis ticks with months
plt.grid(True)

# Show Plot
plt.tight_layout()
plt.show()
