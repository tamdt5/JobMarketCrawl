from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime
import numpy as np
import logging
import time

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def batch_send_to_kafka(df, producer, topic='crawl_res', batch_size=100):
    """Gửi dữ liệu theo batch để tối ưu performance"""
    total_records = len(df)
    sent_count = 0
    
    logger.info(f"Bắt đầu gửi {total_records} records với batch size {batch_size}")
    
    for start_idx in range(0, total_records, batch_size):
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx]
        
        # Xử lý batch
        for index, row in batch_df.iterrows():
            try:
                record = row.to_dict()
                record = {k: None if pd.isna(v) else v for k, v in record.items()}
                record["timestamp_iso"] = int(datetime.utcnow().timestamp() * 1000)
                record["batch_processing_timestamp"] = datetime.utcnow().isoformat()
                
                producer.send(topic, record)
                sent_count += 1
                
            except Exception as e:
                logger.error(f"Lỗi khi gửi record {index}: {e}")
                continue
        
        # Flush batch
        producer.flush()
        
        # Log progress
        progress = (sent_count / total_records) * 100
        logger.info(f"Đã gửi {sent_count}/{total_records} records ({progress:.1f}%)")
        
        # Small delay để tránh overwhelm Kafka
        time.sleep(0.1)
    
    logger.info(f"Hoàn thành gửi {sent_count} records vào Kafka")
    return sent_count

def main():
    """Main function với error handling"""
    try:
        path_data = './jobs.csv'
        
        # Kiểm tra file tồn tại
        import os
        if not os.path.exists(path_data):
            logger.error(f"Không tìm thấy file {path_data}")
            return
        
        # Khởi tạo Kafka producer với config tối ưu
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            batch_size=16384,  # 16KB batch size
            linger_ms=10,      # Wait 10ms for batching
            compression_type='gzip',  # Compress messages
            acks='all',        # Wait for all replicas
            retries=3,         # Retry failed sends
            max_block_ms=10000,  # Max time to block when buffer is full
            request_timeout_ms=30000,  # Request timeout
            api_version=(2, 6, 0)  # Specify API version to avoid compatibility issues
        )
        
        # Đọc dữ liệu
        logger.info(f"Đọc dữ liệu từ {path_data}")
        df = pd.read_csv(path_data)
        logger.info(f"Đã đọc {len(df)} records với {len(df.columns)} columns")
        
        # Gửi theo batch
        sent_count = batch_send_to_kafka(df, producer, batch_size=100)
        
        # Final flush
        producer.flush()
        producer.close()
        
        logger.info(f"✅ Đã gửi thành công {sent_count} records vào Kafka topic 'crawl_res'")
        
    except Exception as e:
        logger.error(f"❌ Lỗi trong quá trình ingestion: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()