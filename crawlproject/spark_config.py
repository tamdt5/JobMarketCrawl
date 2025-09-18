"""
Spark Configuration Settings
Cấu hình cho Apache Spark trong dự án Job Market Crawl
"""

# Spark Configuration
SPARK_CONFIG = {
    # Master URL - có thể thay đổi tùy theo môi trường
    "master_url": "spark://localhost:7077",
    
    # Application settings
    "app_name": "JobMarketDataPreprocessing",
    
    # Performance settings
    "executor_memory": "2g",
    "executor_cores": 2,
    "driver_memory": "1g",
    "max_result_size": "1g",
    
    # SQL settings
    "sql_adaptive_enabled": True,
    "sql_adaptive_coalesce_partitions_enabled": True,
    "sql_adaptive_skew_join_enabled": True,
    
    # Serialization
    "serializer": "org.apache.spark.serializer.KryoSerializer",
    
    # Logging
    "log_level": "WARN",
    
    # Dynamic allocation
    "dynamic_allocation_enabled": True,
    "dynamic_allocation_min_executors": 1,
    "dynamic_allocation_max_executors": 4,
    "dynamic_allocation_initial_executors": 2,
}

# Kafka Configuration
KAFKA_CONFIG = {
    "bootstrap_servers": ["localhost:29092"],
    "topic": "crawl_res",
    "batch_size": 100,
    "linger_ms": 10,
    "compression_type": "gzip",
    "acks": "all",
    "retries": 3,
}

# Data Processing Configuration
DATA_CONFIG = {
    # File paths
    "input_csv_path": "./jobs.csv",
    "output_topic": "crawl_res",
    
    # Processing settings
    "batch_size": 1000,
    "max_records_per_batch": 10000,
    
    # Data quality thresholds
    "min_completeness_score": 50.0,  # Minimum 50% completeness
    "max_null_percentage": 80.0,     # Maximum 80% null values
    
    # Text processing
    "max_text_length": 10000,
    "min_text_length": 10,
    
    # Salary processing - sẽ được tính dynamic từ data
    "max_salary_percentile": 95,  # 95th percentile
    "min_salary_percentile": 5,   # 5th percentile
    "default_max_salary": 100000000,  # 100M VND fallback
    "default_min_salary": 0,
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "spark_processing.log",
    "max_file_size": "10MB",
    "backup_count": 5,
}

# Monitoring Configuration
MONITORING_CONFIG = {
    "enable_metrics": True,
    "metrics_interval": 30,  # seconds
    "enable_profiling": False,
    "profile_output_dir": "./profiles",
}

def get_spark_config():
    """Trả về cấu hình Spark"""
    return SPARK_CONFIG.copy()

def get_kafka_config():
    """Trả về cấu hình Kafka"""
    return KAFKA_CONFIG.copy()

def get_data_config():
    """Trả về cấu hình xử lý dữ liệu"""
    return DATA_CONFIG.copy()

def get_logging_config():
    """Trả về cấu hình logging"""
    return LOGGING_CONFIG.copy()

def get_monitoring_config():
    """Trả về cấu hình monitoring"""
    return MONITORING_CONFIG.copy()

# Environment-specific configurations
DEVELOPMENT_CONFIG = {
    "spark_master": "local[*]",
    "executor_memory": "1g",
    "driver_memory": "512m",
    "log_level": "INFO",
}

PRODUCTION_CONFIG = {
    "spark_master": "spark://spark-master:7077",
    "executor_memory": "4g",
    "driver_memory": "2g",
    "log_level": "WARN",
    "dynamic_allocation_max_executors": 10,
}

def get_environment_config(env="development"):
    """Trả về cấu hình theo môi trường"""
    if env.lower() == "production":
        return PRODUCTION_CONFIG.copy()
    else:
        return DEVELOPMENT_CONFIG.copy()
