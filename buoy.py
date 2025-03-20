# This script retrieves data from the National Data Bouy Center and turns them into APRS objects.
# Executing this script using crontab is recommended.
import socket
import requests
from datetime import datetime, timedelta
import time

def decimal_to_dmd(value, is_latitude):
    degrees = int(abs(value))
    minutes = (abs(value) - degrees) * 60
    direction = "N" if is_latitude and value >= 0 else "S" if is_latitude else "E" if value >= 0 else "W"
    return f"{degrees:02d}{minutes:05.2f}{direction}" if is_latitude else f"{degrees:03d}{minutes:05.2f}{direction}"

def safe_value(value, default="..."):
    return value if value != "MM" else default

def convert_temperature(temp):
    if temp == "...":
        return "..."
    temp_f = int(round(float(temp) * 9 / 5 + 32))
    return f"{temp_f:03d}" if temp_f >= 0 else f"-{abs(temp_f):02d}"

def convert_wind_speed(value):
    if value == "...":
        return "..."
    return f"{int(float(value) * 2.23694):03d}"

def convert_pressure(pressure):
    if pressure == "...":
        return "....."
    return f"{int(float(pressure) * 10):05d}"

def get_latest_buoy_data():
    url = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
    print("Fetching latest buoy data...")
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve latest buoy data.")
        return []
    
    print("Processing buoy data...")
    buoy_data_list = []
    data_lines = response.text.splitlines()
    
    for line in data_lines[2:]:
        if len(line) < 70:
            continue
        
        try:
            fields = line.split()
            if len(fields) < 18:
                print(f"Skipping {line[:7].strip()}: Insufficient data fields.")
                continue
            
            buoy_id, lat, lon, year, month, day, hour, minute = fields[:8]
            wind_dir, wind_speed, wind_gust = fields[8:11]
            pressure, temp = fields[15], fields[17]  # PRES column for pressure, ATMP column for temperature
            
            obs_time = datetime.strptime(f"{year} {month} {day} {hour} {minute}", "%Y %m %d %H %M")
            if datetime.utcnow() - obs_time > timedelta(minutes=30):
                print(f"Skipping {buoy_id}: Data is older than 30 minutes.")
                continue
        except ValueError:
            print(f"Skipping {buoy_id}: Invalid timestamp or data format.")
            continue
        
        wind_speed = convert_wind_speed(safe_value(wind_speed))
        wind_gust = convert_wind_speed(safe_value(wind_gust))
        wind_direction = f"{int(safe_value(wind_dir, '0')):03d}" if safe_value(wind_dir) != "..." else "..."
        temperature = convert_temperature(safe_value(temp))
        pressure = convert_pressure(safe_value(pressure))
        
        if all(value == "..." or value == "....." for value in [wind_speed, wind_gust, wind_direction, temperature, pressure]):
            print(f"Skipping {buoy_id}: No valid weather data.")
            continue
        
        buoy_data_list.append({
            "id": buoy_id.ljust(9),
            "latitude": float(lat),
            "longitude": float(lon),
            "wind_speed": wind_speed,
            "wind_gust": wind_gust,
            "wind_direction": wind_direction,
            "temperature": temperature,
            "pressure": pressure,
            "obs_time": obs_time.strftime("%d%H%M"),
        })
    
    print(f"Total valid buoys: {len(buoy_data_list)}")
    return buoy_data_list

def send_to_aprs(callsign, passcode, buoy_data):
    aprs_host = "wg3k-ca.firenet.us"
    aprs_port = 10155
    
    lat = decimal_to_dmd(buoy_data["latitude"], is_latitude=True)
    lon = decimal_to_dmd(buoy_data["longitude"], is_latitude=False)
    
    aprs_message = f"{callsign}>APFBUO,TCPIP*:;{buoy_data['id']}*{buoy_data['obs_time']}z{lat}/{lon}_" \
                   f"{buoy_data['wind_direction']}/{buoy_data['wind_speed']}g{buoy_data['wind_gust']}t{buoy_data['temperature']}b{buoy_data['pressure']}"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((aprs_host, aprs_port))
        s.sendall(f"user {callsign} pass {passcode} vers Python-Buoy 1.0\n".encode())
        s.sendall(f"{aprs_message}\n".encode())
        print(f"{buoy_data['id']}: Sent to APRS-IS:", aprs_message)
    
    time.sleep(1)  # Rate-limit to 1 packet per second

if __name__ == "__main__":
    CALLSIGN = "CALLSIGN"
    PASSCODE = "PASSCODE"
    
    buoy_data_list = get_latest_buoy_data()
    
    if not buoy_data_list:
        print("No valid buoy data to send.")
    
    for buoy_data in buoy_data_list:
        send_to_aprs(CALLSIGN, PASSCODE, buoy_data)
        print(f"{buoy_data['id']}: Successfully sent to APRS-IS.")
