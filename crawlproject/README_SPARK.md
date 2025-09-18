# Job Market Crawl - Apache Spark Integration

## Tổng quan (Overview)

Dự án này tích hợp Apache Spark để tiền xử lý dữ liệu thị trường việc làm trước khi đưa vào Apache Kafka. Spark được sử dụng để:

- Làm sạch và chuẩn hóa dữ liệu
- Xử lý dữ liệu text và địa điểm
- Chuẩn hóa dữ liệu lương
- Thêm metrics chất lượng dữ liệu
- Xử lý timestamp và yêu cầu công việc

## Kiến trúc (Architecture)

```
Web Scraper → CSV File → Apache Spark → Kafka → Apache Pinot
     ↓              ↓           ↓         ↓         ↓
  vieclamtot_   jobs.csv   Preprocessing  Topic   Analytics
   spider.py              & Cleaning    crawl_res  Dashboard
```

## Cài đặt (Installation)

### 1. Cài đặt Dependencies

```bash
# Cài đặt Python dependencies
make install

# Hoặc thủ công
pip install -r requirements.txt
```

### 2. Khởi động Spark Cluster

```bash
# Khởi động Spark cluster với Docker
make start-spark

# Hoặc thủ công
cd ../docker
docker-compose up -d spark-master spark-worker
```

### 3. Kiểm tra Services

```bash
# Kiểm tra trạng thái services
make status

# Spark Master UI: http://localhost:8080
# Kafka: localhost:29092
```

## Sử dụng (Usage)

### 1. Chạy Pipeline Hoàn Chỉnh

```bash
# Chạy toàn bộ pipeline (scrape + preprocess + kafka)
make run-full-pipeline

# Hoặc từng bước
make run-scraper      # Thu thập dữ liệu mới
make run-pipeline     # Tiền xử lý và gửi vào Kafka
```

### 2. Chỉ Chạy Tiền Xử Lý

```bash
# Chỉ chạy Spark preprocessing
make run-preprocessing

# Hoặc
python spark_preprocessing.py
```

### 3. Chạy Pipeline Tích Hợp

```bash
# Chạy pipeline tích hợp Spark + Kafka
python spark_ingest_to_kafka.py
```

## Cấu hình (Configuration)

### Spark Configuration

File `spark_config.py` chứa các cấu hình:

```python
SPARK_CONFIG = {
    "master_url": "spark://localhost:7077",
    "app_name": "JobMarketDataPreprocessing",
    "executor_memory": "2g",
    "executor_cores": 2,
    "driver_memory": "1g",
    # ... các cấu hình khác
}
```

### Kafka Configuration

```python
KAFKA_CONFIG = {
    "bootstrap_servers": ["localhost:29092"],
    "topic": "crawl_res",
    "batch_size": 100,
    # ... các cấu hình khác
}
```

## Tính năng Tiền Xử Lý (Preprocessing Features)

### 1. Làm Sạch Dữ Liệu Text
- Loại bỏ ký tự đặc biệt
- Chuẩn hóa whitespace
- Xử lý encoding UTF-8

### 2. Chuẩn Hóa Dữ Liệu Lương
- Xử lý min_salary và max_salary
- Tạo cột salary_range
- Xử lý null values

### 3. Xử Lý Địa Điểm
- Tạo full_address từ các cột địa chỉ
- Xử lý tọa độ longitude/latitude
- Chuẩn hóa tên địa điểm

### 4. Xử Lý Timestamp
- Chuyển đổi list_time sang datetime
- Tạo các cột year, month, day
- Thêm processing timestamp

### 5. Xử Lý Yêu Cầu Công Việc
- Tách skills thành array
- Chuẩn hóa experience level
- Xử lý education requirements

### 6. Metrics Chất Lượng Dữ Liệu
- Đếm số cột null
- Tính completeness score
- Thêm data quality metrics

## Monitoring và Logging

### 1. Spark UI
- Truy cập: http://localhost:8080
- Theo dõi jobs, stages, tasks
- Xem metrics performance

### 2. Logging
```bash
# Xem logs
tail -f spark_processing.log

# Hoặc
make monitor-spark
```

### 3. Kafka Monitoring
```bash
# Xem Kafka topics
make monitor-kafka

# Hoặc
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Troubleshooting

### 1. Spark Connection Issues
```bash
# Kiểm tra Spark cluster
curl http://localhost:8080/api/v1/applications

# Restart Spark cluster
make stop-spark
make start-spark
```

### 2. Memory Issues
- Tăng executor memory trong `spark_config.py`
- Giảm batch size trong `DATA_CONFIG`
- Kiểm tra Docker memory limits

### 3. Kafka Connection Issues
```bash
# Kiểm tra Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Restart Kafka
cd ../docker
docker-compose restart kafka
```

## Performance Tuning

### 1. Spark Tuning
- Tăng executor memory và cores
- Sử dụng dynamic allocation
- Tối ưu serialization (Kryo)

### 2. Data Processing Tuning
- Điều chỉnh batch size
- Sử dụng partitioning
- Cache intermediate results

### 3. Kafka Tuning
- Tăng batch size
- Sử dụng compression
- Tối ưu linger time

## Development

### 1. Code Quality
```bash
# Format code
make format

# Lint code
make lint

# Type checking
make type-check

# Run all checks
make check-all
```

### 2. Testing
```bash
# Run tests
make test

# Development setup
make dev-setup
```

## Production Deployment

### 1. Production Configuration
- Cập nhật `spark_config.py` với production settings
- Tăng memory và cores
- Cấu hình monitoring

### 2. Deployment
```bash
# Deploy to production
make deploy-prod
```

## API Reference

### JobDataPreprocessor Class

```python
class JobDataPreprocessor:
    def __init__(self, spark_master_url="spark://localhost:7077")
    def load_data(self, csv_path)
    def clean_text_data(self, df)
    def standardize_salary_data(self, df)
    def process_location_data(self, df)
    def process_timestamp_data(self, df)
    def process_job_requirements(self, df)
    def add_data_quality_metrics(self, df)
    def preprocess_data(self, csv_path)
    def send_to_kafka(self, df, topic="crawl_res")
    def run_preprocessing_pipeline(self, csv_path, kafka_topic="crawl_res")
```

## Contributing

1. Fork the repository
2. Create feature branch
3. Make changes
4. Run tests and checks
5. Submit pull request

## License

MIT License - see LICENSE file for details.
