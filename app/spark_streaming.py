from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

schema = StructType([
    StructField("event_type", StringType()),
    StructField("user_id", IntegerType()),
    StructField("timestamp", LongType()),
    StructField("amount", DoubleType())
])

spark = SparkSession.builder.appName("EcommerceStreaming").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .load()

events = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")
    
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def write_events_to_minio(df, epoch_id):
    df.write.mode("append").json("s3a://anomalies-bucket/events/")

events.writeStream \
    .foreachBatch(write_events_to_minio) \
    .outputMode("append") \
    .start() \
    .awaitTermination()