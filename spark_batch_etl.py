from pyspark.sql import SparkSession

class SparkBatchETL:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SparkBatchETL") \
            .enableHiveSupport() \
            .getOrCreate()

    def run_etl(self):
        csv_df = self.spark.read.csv("generated_csv/", header=True, inferSchema=True)

        mysql_df = self.spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/sensor_db") \
            .option("dbtable", "sensor_logs") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        final_df = csv_df.join(mysql_df, csv_df.City == mysql_df.device_id, "inner")

        final_df.write.mode("overwrite").saveAsTable("final_table")
        final_df.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/sensor_db") \
            .option("dbtable", "final_table") \
            .option("user", "user") \
            .option("password", "password") \
            .save()

if __name__ == "__main__":
    etl = SparkBatchETL()
    etl.run_etl()
