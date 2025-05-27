try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError as e:
    raise ImportError("PySpark is not installed. Please install it using 'pip install pyspark'") from e

from modular_processor import ModularStreamingProcessor
from monolithic_processor import MonolithicStreamingProcessor
from hybrid_processor import HybridStreamingProcessor
from config import OUTPUT_PATH, CHECKPOINT_PATH

if __name__ == "__main__":
    spark = SparkSession.builder.appName("StreamingPOC").getOrCreate()

    modular = ModularStreamingProcessor(spark)
    monolithic = MonolithicStreamingProcessor(spark)
    hybrid = HybridStreamingProcessor(spark)

    modular_query = modular.run_pipeline(OUTPUT_PATH, CHECKPOINT_PATH + "/modular")
    monolithic_query = monolithic.run_complete_streaming_pipeline(OUTPUT_PATH, CHECKPOINT_PATH + "/monolithic")
    hybrid_query = hybrid.run_pipeline(OUTPUT_PATH, CHECKPOINT_PATH + "/hybrid")

    modular_query.awaitTermination()
    monolithic_query.awaitTermination()
    hybrid_query.awaitTermination()