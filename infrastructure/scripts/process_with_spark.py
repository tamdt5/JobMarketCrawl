from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, window, count, expr, lit, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

schema = StructType([
    StructField("ad_id", StringType(), True),
    StructField("list_id", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("category", StringType(), True),
    StructField("min_salary", FloatType(), True),
    StructField("max_salary", FloatType(), True),
    StructField("timestamp_iso", LongType(), True)
])

def categorize_salary(avg_salary):
    if avg_salary is None:
        return 'Undefined'
    if avg_salary < 10000000:
        return 'Entry'
    elif 10000000 <= avg_salary < 25000000:
        return 'Mid-level'
    else:
        return 'Senior'

salary_categorizer_udf = udf(categorize_salary, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder.appName("JobPostingsProcessor").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_job_postings") \
        .option("startingOffsets", "latest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")

    transformed_df = json_df \
        .withColumn("min_salary", col("min_salary").cast(FloatType())) \
        .withColumn("max_salary", col("max_salary").cast(FloatType())) \
        .na.fill(value=0, subset=["min_salary", "max_salary"]) \
        .withColumn("average_salary", (col("min_salary") + col("max_salary")) / 2) \
        .withColumn("salary_tier", salary_categorizer_udf(col("average_salary"))) \
        .withColumn("success", lit(1)) \
        .withColumn("failure", lit(0))

print("🚀 Ready for ingesting data...", flush=True)

# ========== Count per microbatch ==========
from pyspark.sql.functions import current_timestamp

batch_count_query = transformed_df \
    .withColumn("batch_time", current_timestamp()) \
    .groupBy(window(col("batch_time"), "5 seconds")) \
    .agg(
        count("*").alias("processed_records"),
        spark_sum("success").alias("success_records"),
        spark_sum("failure").alias("failure_records")
    ) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# ========== Kafka sink ==========
kafka_query = transformed_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "cleansed_job_postings") \
    .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/job_postings_stream") \
    .start()

batch_count_query.awaitTermination()
kafka_query.awaitTermination()
