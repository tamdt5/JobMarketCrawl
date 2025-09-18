"""
Enhanced Data Ingestion Pipeline with Spark Preprocessing
Pipeline tiền xử lý dữ liệu nâng cao với Apache Spark trước khi đưa vào Kafka
"""

import os
import sys
import logging
from datetime import datetime
from spark_preprocessing import JobDataPreprocessor

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SparkKafkaIngestionPipeline:
    """Pipeline tích hợp Spark preprocessing với Kafka ingestion"""
    
    def __init__(self, spark_master_url="spark://localhost:7077"):
        """Khởi tạo pipeline"""
        self.spark_master_url = spark_master_url
        self.preprocessor = None
        
    def validate_environment(self):
        """Kiểm tra môi trường và dependencies"""
        logger.info("Kiểm tra môi trường...")
        
        # Kiểm tra file CSV
        csv_path = "./jobs.csv"
        if not os.path.exists(csv_path):
            logger.error(f"Không tìm thấy file {csv_path}")
            return False
        
        # Kiểm tra kích thước file
        file_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
        logger.info(f"Kích thước file CSV: {file_size:.2f} MB")
        
        return True
    
    def run_pipeline(self, csv_path="./jobs.csv", kafka_topic="crawl_res"):
        """Chạy toàn bộ pipeline"""
        start_time = datetime.now()
        logger.info(f"Bắt đầu pipeline lúc {start_time}")
        
        try:
            # Kiểm tra môi trường
            if not self.validate_environment():
                raise Exception("Môi trường không hợp lệ")
            
            # Khởi tạo preprocessor
            logger.info("Khởi tạo Spark preprocessor...")
            self.preprocessor = JobDataPreprocessor(self.spark_master_url)
            
            # Chạy preprocessing và gửi vào Kafka
            logger.info("Bắt đầu quá trình tiền xử lý và gửi vào Kafka...")
            processed_df = self.preprocessor.run_preprocessing_pipeline(csv_path, kafka_topic)
            
            # Thống kê cuối cùng
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=== PIPELINE HOÀN THÀNH ===")
            logger.info(f"Thời gian xử lý: {duration}")
            logger.info(f"Số records đã xử lý: {processed_df.count()}")
            logger.info(f"Topic Kafka: {kafka_topic}")
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi trong pipeline: {str(e)}")
            return False
        
        finally:
            # Cleanup
            if self.preprocessor:
                try:
                    self.preprocessor.spark.stop()
                    self.preprocessor.kafka_producer.close()
                except:
                    pass

def main():
    """Hàm main"""
    logger.info("=== SPARK KAFKA INGESTION PIPELINE ===")
    
    # Khởi tạo pipeline
    pipeline = SparkKafkaIngestionPipeline()
    
    # Chạy pipeline
    success = pipeline.run_pipeline()
    
    if success:
        logger.info("Pipeline hoàn thành thành công!")
        sys.exit(0)
    else:
        logger.error("Pipeline thất bại!")
        sys.exit(1)

if __name__ == "__main__":
    main()
