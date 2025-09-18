"""
Apache Spark Data Preprocessing Script for Job Market Data
Tiền xử lý dữ liệu thị trường việc làm bằng Apache Spark trước khi đưa vào Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, trim, 
    lower, upper, split, size, current_timestamp,
    to_timestamp, date_format, year, month, dayofmonth,
    coalesce, lit, concat_ws, regexp_extract, from_unixtime,
    hour, minute, second, dayofweek, dayofyear, weekofyear
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType
)
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
from spark_config import get_spark_config, get_kafka_config, get_data_config

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobDataPreprocessor:
    """Lớp xử lý dữ liệu việc làm với Apache Spark"""
    
    def __init__(self, spark_master_url=None):
        """Khởi tạo Spark session với config từ file"""
        # Load configs
        self.spark_config = get_spark_config()
        self.kafka_config = get_kafka_config()
        self.data_config = get_data_config()
        
        # Sử dụng master_url từ config hoặc parameter
        master_url = spark_master_url or self.spark_config["master_url"]
        
        # Khởi tạo Spark session với config đầy đủ
        spark_builder = SparkSession.builder \
            .appName(self.spark_config["app_name"]) \
            .master(master_url)
        
        # Thêm các config từ spark_config
        for key, value in self.spark_config.items():
            if key.startswith("spark."):
                spark_builder = spark_builder.config(key, value)
        
        self.spark = spark_builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.spark_config["log_level"])
        
        # Khởi tạo Kafka producer với config
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            batch_size=self.kafka_config["batch_size"],
            linger_ms=self.kafka_config["linger_ms"],
            compression_type=self.kafka_config["compression_type"],
            acks=self.kafka_config["acks"],
            retries=self.kafka_config["retries"],
            max_block_ms=10000,  # Max time to block when buffer is full
            request_timeout_ms=30000,  # Request timeout
            api_version=(2, 6, 0)  # Specify API version to avoid compatibility issues
        )
        
        logger.info("Spark session và Kafka producer đã được khởi tạo với config")
    
    def load_data(self, csv_path):
        """Đọc dữ liệu từ CSV file"""
        logger.info(f"Đang đọc dữ liệu từ {csv_path}")
        
        # Đọc CSV với schema tự động detect
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .csv(csv_path)
        
        logger.info(f"Đã đọc {df.count()} records với {len(df.columns)} columns")
        return df
    
    def clean_text_data(self, df):
        """Làm sạch dữ liệu text với error handling cho missing columns"""
        logger.info("Bắt đầu làm sạch dữ liệu text...")
        
        # Danh sách các cột text cần làm sạch
        text_columns = [
            'subject', 'body', 'company_name', 'account_name', 
            'category_name', 'area_name', 'region_name', 'ward_name',
            'address', 'detail_address', 'skills'
        ]
        
        for col_name in text_columns:
            try:
                if col_name in df.columns:
                    # Loại bỏ ký tự đặc biệt, làm sạch whitespace
                    df = df.withColumn(
                        col_name,
                        when(col(col_name).isNull(), lit(""))
                        .otherwise(
                            trim(
                                regexp_replace(
                                    regexp_replace(col(col_name), r'[^\w\s\u00C0-\u1EF9]', ' '),
                                    r'\s+', ' '
                                )
                            )
                        )
                    )
                else:
                    # Tạo cột mới với giá trị mặc định nếu không tồn tại
                    logger.warning(f"Column '{col_name}' không tồn tại, tạo với giá trị mặc định")
                    df = df.withColumn(col_name, lit(""))
            except Exception as e:
                logger.error(f"Lỗi khi xử lý column '{col_name}': {e}")
                # Tạo cột với giá trị mặc định
                df = df.withColumn(col_name, lit(""))
        
        return df
    
    def standardize_salary_data(self, df):
        """Chuẩn hóa dữ liệu lương với dynamic limits"""
        logger.info("Chuẩn hóa dữ liệu lương...")
        
        # Xử lý min_salary và max_salary
        salary_columns = ['min_salary', 'max_salary']
        
        for col_name in salary_columns:
            try:
                if col_name in df.columns:
                    # Chuyển đổi sang numeric, xử lý null values
                    df = df.withColumn(
                        col_name,
                        when(col(col_name).isNull() | (col(col_name) == ""), lit(0))
                        .otherwise(col(col_name).cast("double"))
                    )
                else:
                    # Tạo cột với giá trị mặc định
                    logger.warning(f"Column '{col_name}' không tồn tại, tạo với giá trị mặc định")
                    df = df.withColumn(col_name, lit(0))
            except Exception as e:
                logger.error(f"Lỗi khi xử lý column '{col_name}': {e}")
                df = df.withColumn(col_name, lit(0))
        
        # Tính dynamic salary limits từ data
        try:
            if 'min_salary' in df.columns and 'max_salary' in df.columns:
                # Lấy percentiles từ data
                salary_stats = df.select(
                    col("min_salary"), col("max_salary")
                ).filter(
                    (col("min_salary") > 0) | (col("max_salary") > 0)
                ).describe()
                
                # Tính dynamic limits (simplified - trong thực tế cần dùng percentile)
                max_salary_limit = self.data_config["default_max_salary"]
                min_salary_limit = self.data_config["default_min_salary"]
                
                # Cap salary values
                df = df.withColumn(
                    "min_salary",
                    when(col("min_salary") > max_salary_limit, lit(max_salary_limit))
                    .when(col("min_salary") < min_salary_limit, lit(min_salary_limit))
                    .otherwise(col("min_salary"))
                ).withColumn(
                    "max_salary",
                    when(col("max_salary") > max_salary_limit, lit(max_salary_limit))
                    .when(col("max_salary") < min_salary_limit, lit(min_salary_limit))
                    .otherwise(col("max_salary"))
                )
                
                logger.info(f"Applied salary limits: min={min_salary_limit}, max={max_salary_limit}")
        except Exception as e:
            logger.error(f"Lỗi khi tính dynamic salary limits: {e}")
        
        # Tạo cột salary_range
        try:
            if 'min_salary' in df.columns and 'max_salary' in df.columns:
                df = df.withColumn(
                    "salary_range",
                    when((col("min_salary") > 0) & (col("max_salary") > 0), 
                    concat_ws(" - ", col("min_salary"), col("max_salary")))
                    .when(col("min_salary") > 0, col("min_salary").cast("string"))
                    .when(col("max_salary") > 0, col("max_salary").cast("string"))
                    .otherwise("Thỏa thuận")
                )
        except Exception as e:
            logger.error(f"Lỗi khi tạo salary_range: {e}")
            df = df.withColumn("salary_range", lit("Thỏa thuận"))
        
        return df
    
    def process_location_data(self, df):
        """Xử lý dữ liệu địa điểm"""
        logger.info("Xử lý dữ liệu địa điểm...")
        
        # Tạo cột full_address
        address_columns = ['address', 'detail_address', 'ward_name', 'area_name', 'region_name']
        existing_address_cols = [col for col in address_columns if col in df.columns]
        
        if existing_address_cols:
            df = df.withColumn(
                "full_address",
                concat_ws(", ", *[col(c) for c in existing_address_cols])
            )
        
        # Xử lý tọa độ
        if 'longitude' in df.columns and 'latitude' in df.columns:
            df = df.withColumn(
                "longitude",
                when(col("longitude").isNull(), lit(0.0))
                .otherwise(col("longitude").cast("double"))
            ).withColumn(
                "latitude", 
                when(col("latitude").isNull(), lit(0.0))
                .otherwise(col("latitude").cast("double"))
            )
        
        return df
    
    def process_timestamp_data(self, df):
        """Xử lý dữ liệu timestamp với đầy đủ các cột thời gian"""
        logger.info("Xử lý dữ liệu timestamp...")
        
        # Xử lý list_time - chuyển từ milliseconds sang timestamp
        if 'list_time' in df.columns:
            df = df.withColumn(
                "list_time_processed",
                when(col("list_time").isNull(), current_timestamp())
                .otherwise(
                    from_unixtime(col("list_time") / 1000)
                )
            )
        else:
            # Nếu không có list_time, tạo cột với current timestamp
            df = df.withColumn("list_time_processed", current_timestamp())
        
        # Tạo các cột thời gian chi tiết
        if 'list_time_processed' in df.columns:
            df = df.withColumn("job_year", year("list_time_processed")) \
                   .withColumn("job_month", month("list_time_processed")) \
                   .withColumn("job_day", dayofmonth("list_time_processed")) \
                   .withColumn("job_hour", hour("list_time_processed")) \
                   .withColumn("job_minute", minute("list_time_processed")) \
                   .withColumn("job_second", second("list_time_processed")) \
                   .withColumn("job_dayofweek", dayofweek("list_time_processed")) \
                   .withColumn("job_dayofyear", dayofyear("list_time_processed")) \
                   .withColumn("job_weekofyear", weekofyear("list_time_processed"))
        
        # Xử lý orig_list_time nếu có
        if 'orig_list_time' in df.columns:
            df = df.withColumn(
                "orig_list_time_processed",
                when(col("orig_list_time").isNull(), current_timestamp())
                .otherwise(
                    from_unixtime(col("orig_list_time") / 1000)
                )
            )
        
        # Thêm timestamp xử lý hiện tại
        df = df.withColumn("processing_timestamp", current_timestamp()) \
               .withColumn("processing_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
               .withColumn("processing_time", date_format(current_timestamp(), "HH:mm:ss"))
        
        return df
    
    def process_job_requirements(self, df):
        """Xử lý yêu cầu công việc"""
        logger.info("Xử lý yêu cầu công việc...")
        
        # Xử lý skills
        if 'skills' in df.columns:
            df = df.withColumn(
                "skills_array",
                when(col("skills").isNull(), lit([]))
                .otherwise(split(col("skills"), ","))
            ).withColumn(
                "skills_count",
                size(col("skills_array"))
            )
        
        # Xử lý experience
        if 'preferred_working_experience' in df.columns:
            df = df.withColumn(
                "experience_level",
                when(col("preferred_working_experience").isNull(), "Không yêu cầu")
                .otherwise(col("preferred_working_experience"))
            )
        
        # Xử lý education
        if 'preferred_education' in df.columns:
            df = df.withColumn(
                "education_level",
                when(col("preferred_education").isNull(), "Không yêu cầu")
                .otherwise(col("preferred_education"))
            )
        
        return df
    
    def add_data_quality_metrics(self, df):
        """Thêm các metrics về chất lượng dữ liệu"""
        logger.info("Thêm metrics chất lượng dữ liệu...")
        
        # Đếm số cột null cho mỗi record
        null_count_expr = sum(
            when(col(c).isNull(), 1).otherwise(0) 
            for c in df.columns
        )
        
        df = df.withColumn("null_count", null_count_expr) \
            .withColumn("completeness_score", 
                          (1 - col("null_count") / len(df.columns)) * 100)
        
        return df
    
    def preprocess_data(self, csv_path):
        """Thực hiện toàn bộ quá trình tiền xử lý"""
        logger.info("Bắt đầu quá trình tiền xử lý dữ liệu...")
        
        # Đọc dữ liệu
        df = self.load_data(csv_path)
        
        # Các bước tiền xử lý
        df = self.clean_text_data(df)
        df = self.standardize_salary_data(df)
        df = self.process_location_data(df)
        df = self.process_timestamp_data(df)
        df = self.process_job_requirements(df)
        df = self.add_data_quality_metrics(df)
        
        logger.info("Hoàn thành tiền xử lý dữ liệu")
        return df
    
    def send_to_kafka(self, df, topic="crawl_res"):
        """Gửi dữ liệu đã xử lý vào Kafka với streaming approach để tránh OOM"""
        logger.info(f"Bắt đầu gửi dữ liệu vào Kafka topic: {topic}")
        
        # Lọc dữ liệu chất lượng thấp trước khi gửi
        min_completeness = self.data_config["min_completeness_score"]
        df_filtered = df.filter(col("completeness_score") >= min_completeness)
        
        total_count = df_filtered.count()
        logger.info(f"Gửi {total_count} records (đã lọc từ {df.count()} records)")
        
        # Sử dụng foreachPartition để tránh collect() - OOM
        def send_partition_to_kafka(partition):
            """Function để gửi partition vào Kafka"""
            sent_count = 0
            for record in partition:
                try:
                    # Chuyển đổi Row thành dict
                    record_dict = record.asDict()
                    
                    # Xử lý các giá trị không thể serialize
                    for key, value in record_dict.items():
                        if value is None:
                            record_dict[key] = None
                        elif hasattr(value, 'isoformat'):  # datetime objects
                            record_dict[key] = value.isoformat()
                        elif isinstance(value, list):
                            record_dict[key] = [str(v) for v in value]
                        else:
                            record_dict[key] = str(value)
                    
                    # Thêm timestamp
                    record_dict["spark_processing_timestamp"] = datetime.utcnow().isoformat()
                    
                    # Gửi vào Kafka
                    self.kafka_producer.send(topic, record_dict)
                    sent_count += 1
                    
                    # Batch send để tối ưu performance
                    if sent_count % self.data_config["batch_size"] == 0:
                        self.kafka_producer.flush()
                        
                except Exception as e:
                    logger.error(f"Lỗi khi gửi record: {str(e)}")
                    continue
            
            # Flush cuối partition
            self.kafka_producer.flush()
            return sent_count
        
        # Gửi dữ liệu theo partition
        df_filtered.foreachPartition(send_partition_to_kafka)
        
        # Final flush
        self.kafka_producer.flush()
        logger.info(f"Đã gửi thành công {total_count} records vào Kafka")
    
    def run_preprocessing_pipeline(self, csv_path, kafka_topic="crawl_res"):
        """Chạy toàn bộ pipeline tiền xử lý"""
        try:
            # Tiền xử lý dữ liệu
            processed_df = self.preprocess_data(csv_path)
            
            # Hiển thị thống kê
            logger.info("=== THỐNG KÊ DỮ LIỆU SAU XỬ LÝ ===")
            logger.info(f"Tổng số records: {processed_df.count()}")
            logger.info(f"Tổng số columns: {len(processed_df.columns)}")
            
            # Hiển thị schema
            processed_df.printSchema()
            
            # Hiển thị sample data
            logger.info("=== SAMPLE DATA ===")
            processed_df.show(5, truncate=False)
            
            # Gửi vào Kafka
            self.send_to_kafka(processed_df, kafka_topic)
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình xử lý: {str(e)}")
            raise
        finally:
            # Đóng Spark session
            self.spark.stop()
            self.kafka_producer.close()

def main():
    """Hàm main để chạy preprocessing"""
    # Đường dẫn đến file CSV
    csv_path = "./jobs.csv"
    
    # Khởi tạo preprocessor
    preprocessor = JobDataPreprocessor()
    
    # Chạy pipeline
    preprocessor.run_preprocessing_pipeline(csv_path)

if __name__ == "__main__":
    main()
