import os

try:
    from pyspark.sql.functions import from_json, col, current_timestamp, hour, window, count
except ModuleNotFoundError as e:
    raise ImportError("PySpark is not installed. Please install it using 'pip install pyspark'") from e

from config import SCHEMA

class HybridStreamingProcessor:
    def __init__(self, spark):
        self.spark = spark

    def create_streaming_source(self):
        return self.spark.readStream.format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .load()

    def _parse_and_enrich(self, df):
        return df.select(from_json(col("value"), SCHEMA).alias("data")) \
            .select("data.*") \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("hour", hour(col("timestamp")))

    def _filter_and_aggregate(self, df):
        return df.filter(col("event_type").isin(["purchase", "view", "click"])) \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(window(col("timestamp"), "5 minutes")) \
            .agg(count("*").alias("event_count"))

    def run_pipeline(self, output_path, checkpoint_path):
        df = self.create_streaming_source()
        df = df.transform(self._parse_and_enrich).transform(self._filter_and_aggregate)

        df.explain(mode="extended")

        os.makedirs("plans", exist_ok=True)
        with open("plans/hybrid_plan.txt", "w") as f:
            f.write(df._jdf.queryExecution().toString())

        # Start the streaming query
        query = df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("checkpointLocation", checkpoint_path) \
            .start()

        return query