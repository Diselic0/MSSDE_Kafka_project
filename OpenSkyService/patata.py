from kafka import KafkaProducer
import json
import time
import requests

# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_opensky_data():
    url = 'https://opensky-network.org/api/states/all?lamin=42.2&lamax=49.8&lomin=-4.7&lomax=10.9 '
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data from OpenSky API: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception occurred while fetching data: {e}")
        return None

def process_and_send_data(producer, data):
    states = data.get('states', [])
    for state in states:
        # Extract only the specified fields
        aircraft_data = {
            'icao24': state[0],
            'callsign': state[1].strip() if state[1] else None,
            'longitude': state[5],
            'latitude': state[6],
            'baro_altitude': state[7],
        }
        
        # Only proceed if latitude and longitude are available
        if aircraft_data['latitude'] is not None and aircraft_data['longitude'] is not None:
            # Send data to the 'aircraft-data' topic
            producer.send('quadrant-NW', value=aircraft_data)
            print(f"Sent data: {aircraft_data}")
        else:
            print(f"Skipping aircraft {aircraft_data['icao24']} due to missing position data.")

def produce_data():
    while True:
        data = fetch_opensky_data()
        if data:
            process_and_send_data(producer, data)
            producer.flush()
        else:
            print("No data fetched.")
        time.sleep(5)  # Fetch data every 60 seconds

if __name__ == "__main__":
    produce_data()
