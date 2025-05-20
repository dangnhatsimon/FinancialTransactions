from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StringType, StructField, DataType
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from FinancialTransactions.config.config import configuration
import logging
from pyspark.conf import SparkConf


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

KAFKA_BROKER = "broker:29092"
S3_BUCKET = "s3a://spark-kafka-smart-city"


def read_kafka_topic(
    spark: SparkSession,
    topic: str,
    schema: DataType,
    broker: str
) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "2 minutes")
    )


def stream_writer(
    input: DataFrame,
    checkpoint_location: str,
    output: str,
    mode: str = "append"
):
    return (
        input.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output)
        .outputMode(mode)
        .start()
    )


def main():

    

    sc = spark.sparkContext
    # sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

    sc.setLogLevel("WARN")


if __name__ == "__main__":
    KAFKA_BROKER = ["localhost:29092", "localhost:39092", "localhost:49092"]
    SRC_TOPIC = "financial_transactions"
    AGGREGATES_TOPIC = "transaction_aggregates"
    ANOMALIES_TOPIC = "transaction_anomalies"
    CHECKPOINT_DIR = "/mnt/spark-checkpoints"
    STATES_DIR = "/mnt/spark-state"
    spark = (
        SparkSession.builder.appName("FinancialTransactions")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR)
        .config("spark.sql.shuffle.partitions", 20)
        .getOrCreate()
    )
    
# docker exec -it smartcity-spark-master-1 spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark_city.py