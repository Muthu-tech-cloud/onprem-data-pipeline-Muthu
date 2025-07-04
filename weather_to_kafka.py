import requests
from kafka import KafkaProducer
import json
import time
import os

class WeatherKafkaProducer:
    def __init__(self, city, api_key, kafka_server, topic):
        self.city = city
        self.api_key = api_key
        self.kafka_server = kafka_server
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_server],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def get_weather(self):
        url = f"http://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={self.api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            print("Weather API fetched successfully")
            return response.json()
        else:
            print(f"Failed to fetch weather data: {response.status_code}")
            return {}

    def send_to_kafka(self, data):
        self.producer.send(self.topic, data)
        self.producer.flush()

if __name__ == "__main__":
    api_key = os.getenv("API_KEY", "a")
    kafka = WeatherKafkaProducer("Chennai", api_key, "localhost:9092", "weather-topic")
    data = kafka.get_weather()
    kafka.send_to_kafka(data)
