# scripts/3_process_with_spark.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType, LongType, ArrayType

# Định nghĩa cấu trúc của dữ liệu JSON đọc từ Kafka
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
    # Thêm các trường khác nếu bạn cần
])

# Hàm UDF (User Defined Function) để phân loại mức lương
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
    spark = SparkSession \
        .builder \
        .appName("JobPostingsProcessor") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Đọc dữ liệu streaming từ topic "raw_job_postings"
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_job_postings") \
        .option("startingOffsets", "latest") \
        .load()

    # Chuyển đổi dữ liệu từ dạng binary của Kafka sang JSON
    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")

    # === BƯỚC TRANSFORMATION (LÀM SẠCH & LÀM GIÀU) ===
    transformed_df = json_df \
        .withColumn("min_salary", col("min_salary").cast(FloatType())) \
        .withColumn("max_salary", col("max_salary").cast(FloatType())) \
        .na.fill(value=0, subset=["min_salary", "max_salary"]) \
        .withColumn("average_salary", (col("min_salary") + col("max_salary")) / 2) \
        .withColumn("salary_tier", salary_categorizer_udf(col("average_salary")))

    # Ghi dữ liệu đã xử lý vào topic "cleansed_job_postings"
    # Dữ liệu phải được chuyển về dạng JSON string trong cột "value"
    query = transformed_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "cleansed_job_postings") \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    query.awaitTermination()