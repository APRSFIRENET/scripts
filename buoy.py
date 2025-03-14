import socket
import requests
from datetime import datetime, timedelta
import time

def get_time_offset():
    return datetime.utcnow() - datetime.now()

def decimal_to_dmd(value, is_latitude):
    degrees = int(abs(value))
    minutes = (abs(value) - degrees) * 60
    if is_latitude:
        direction = "N" if value >= 0 else "S"
        return f"{degrees:02d}{minutes:05.2f}{direction}"
    else:
        direction = "E" if value >= 0 else "W"
        return f"{degrees:03d}{minutes:05.2f}{direction}"

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
            if datetime.utcnow() - obs_time > timedelta(hours=0.5):
                print(f"Skipping {buoy_id}: Data is older than 30 minutes.")
                continue
        except ValueError:
            print(f"Skipping {buoy_id}: Invalid timestamp or data format.")
            continue
        
        def safe_value(value, default="..."):
            return value if value != "MM" else default
        
        temp = safe_value(temp)
        if temp != "...":
            temp = int(round(float(temp) * 9/5 + 32))  # Convert °C to °F and round to whole number
            temp = f"{temp:03d}" if temp >= 0 else f"-{abs(temp):02d}"  # Ensure three-character field with '-' for negatives
        
        wind_speed = safe_value(wind_speed)
        if wind_speed != "...":
            wind_speed = f"{int(float(safe_value(wind_speed, '0')) * 2.23694):03d}"  # Convert m/s to mph, whole number, 3 chars
        
        wind_gust = safe_value(wind_gust)
        if wind_gust != "...":
            wind_gust = f"{int(float(safe_value(wind_gust, '0')) * 2.23694):03d}"  # Convert m/s to mph, whole number, 3 chars
        
        wind_dir = safe_value(wind_dir)
        if wind_dir != "...":
            wind_dir = f"{int(safe_value(wind_dir, '0')):03d}"  # Ensure three-character field
        
        try:
            pressure = f"{int(float(pressure) * 10):05d}" if pressure != "..." else "....."  # Convert to tenths of millibars and ensure 5-character field
        except ValueError:
            pressure = "....."

        buoy_data_list.append({
            "id": buoy_id.ljust(9),  # Ensure ID is exactly 9 characters with trailing spaces if needed
            "latitude": float(lat),
            "longitude": float(lon),
            "wind_speed": wind_speed,
            "wind_gust": wind_gust,
            "wind_direction": wind_dir,
            "temperature": temp,
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
    wind_speed = buoy_data["wind_speed"]
    wind_gust = buoy_data["wind_gust"]
    wind_dir = buoy_data["wind_direction"]
    temp = buoy_data["temperature"]
    pressure = buoy_data["pressure"]
    obs_time = buoy_data["obs_time"]
    
    aprs_message = f"{callsign}>APFBUO,TCPIP*:;{buoy_data['id']}*{obs_time}z{lat}/{lon}_" \
                   f"{wind_dir}/{wind_speed}g{wind_gust}t{temp}b{pressure}"
    
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

