from kafka import KafkaProducer
import json
import requests

# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Message to send
def fetch_opensky_data():
    #url = 'https://opensky-network.org/api/states/all?lamin=42.2&lamax=49.8&lomin=-4.7&lomax=10.9 '
    #Gets the information of a particular airplane
    url = 'https://opensky-network.org/api/states/all?icao24=a8bca7'

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data from OpenSky API: {response.status_code}")
        return None

message = fetch_opensky_data()

# Send the message to the 'quadrant-NE' topic
producer.send('quadrant-NW', value=message)

# Flush to ensure all messages are sent
producer.flush()

print("Message sent to Kafka topic 'quadrant-NW':", message)
