from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime
from tqdm import tqdm
import os
import glob
import concurrent.futures
import platform

# Thêm thư viện để phân tích cú pháp ngày tháng
from dateutil import parser

DATA_DIR = 'data'

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Hàm trợ giúp để chuyển đổi ISO string sang epoch milliseconds
def iso_to_epoch_ms(iso_str):
    if pd.isna(iso_str):
        return int(datetime.utcnow().timestamp() * 1000)
    try:
        dt_obj = parser.isoparse(iso_str)
        return int(dt_obj.timestamp() * 1000)
    except (ValueError, TypeError):
        return int(datetime.utcnow().timestamp() * 1000)

def process_file(file_path):
    """Đọc và đẩy dữ liệu từ một file CSV vào Kafka."""
    try:
        df = pd.read_csv(file_path)
        print(f"Reading data from {file_path}...")

        futures = []
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Sending {file_path} to Kafka"):
            record = row.to_dict()
            record = {k: None if pd.isna(v) else v for k, v in record.items()}
            original_iso_timestamp = record.get("timestamp_iso")
            record["timestamp_iso"] = iso_to_epoch_ms(original_iso_timestamp)
            message_key = record.get('list_id')
            future = producer.send('raw_job_postings', key=message_key, value=record)
            futures.append(future)

        # Đợi tất cả message được gửi
        for future in futures:
            future.get(timeout=10)  # Chờ tối đa 10 giây mỗi message
        print(f"All data from {file_path} sent to Kafka topic 'raw_job_postings'!")
        return True
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def ingest_to_kafka():
    """Đọc tất cả file CSV trong thư mục data và đẩy lên Kafka."""
    csv_files = glob.glob(os.path.join(DATA_DIR, 'jobs_*.csv'))
    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}. Please run 'make crawl' first.")
        return

    print(f"Found {len(csv_files)} CSV files: {csv_files}")
    
    system_name = platform.system()
    print(f"Detected OS: {system_name}")

    if system_name == "Windows":
        # Windows: chạy tuần tự
        print("Running in sequential mode (Windows detected)...")
        for csv_file in csv_files:
            process_file(csv_file)
    else:
        # Linux/Mac: chạy bất đồng bộ
        print("Running in parallel mode (non-Windows detected)...")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_file, csv_file) for csv_file in csv_files]
            concurrent.futures.wait(futures)

if __name__ == "__main__":
    try:
        ingest_to_kafka()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing Kafka producer...")
        producer.flush()
        producer.close()