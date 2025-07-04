from faker import Faker
import csv
import os
from datetime import datetime

class FakeWeatherLogger:
    def __init__(self, output_dir="generated_csv"):
        self.fake = Faker()
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate(self):
        filename = f"weather_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Name", "City", "Temperature"])
            for _ in range(10):
                writer.writerow([
                    self.fake.name(),
                    self.fake.city(),
                    self.fake.random_int(min=10, max=40)
                ])

if __name__ == "__main__":
    logger = FakeWeatherLogger()
    logger.generate()
