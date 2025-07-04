from faker import Faker
import mysql.connector

class MockMySQLInserter:
    def __init__(self):
        self.fake = Faker()
        self.ensure_database()
        self.conn = mysql.connector.connect(
            host="localhost", user="root", password="root", database="sensor_db"
        )
        self.cursor = self.conn.cursor()

    def ensure_database(self):
        temp_conn = mysql.connector.connect(
            host="localhost", user="root", password="root"
        )
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute("CREATE DATABASE IF NOT EXISTS sensor_db")
        temp_conn.commit()
        temp_cursor.close()
        temp_conn.close()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_logs (
                device_id VARCHAR(10),
                temp INT,
                humidity INT
            )
        """)

    def insert_data(self):
        for _ in range(10):
            self.cursor.execute(
                "INSERT INTO sensor_logs VALUES (%s, %s, %s)",
                (
                    self.fake.bothify(text="DEV###"),
                    self.fake.random_int(20, 35),
                    self.fake.random_int(30, 90)
                )
            )
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    inserter = MockMySQLInserter()
    inserter.create_table()
    inserter.insert_data()
    inserter.close()
