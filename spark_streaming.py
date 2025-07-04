from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class KafkaToParquetStreamer:
    def __init__(self, topic, kafka_server, output_path, checkpoint_path):
        self.spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()
        self.topic = topic
        self.kafka_server = kafka_server
        self.output_path = output_path
        self.checkpoint_path = checkpoint_path

    def run(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("main", StructType([
                StructField("temp", DoubleType())
            ]))
        ])

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.topic) \
            .load()

        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        query = parsed_df.writeStream \
            .format("parquet") \
            .option("path", self.output_path) \
            .option("checkpointLocation", self.checkpoint_path) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    streamer = KafkaToParquetStreamer(
        topic="weather-topic",
        kafka_server="localhost:9092",
        output_path="data/parquet/",
        checkpoint_path="/tmp/checkpoint/"
    )
    streamer.run()
