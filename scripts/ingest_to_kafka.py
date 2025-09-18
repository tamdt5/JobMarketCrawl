from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime
from tqdm import tqdm

# Thêm thư viện để phân tích cú pháp ngày tháng
from dateutil import parser

path_data = 'data/jobs.csv'

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Hàm trợ giúp để chuyển đổi ISO string sang epoch milliseconds
def iso_to_epoch_ms(iso_str):
    if pd.isna(iso_str):
        return int(datetime.utcnow().timestamp() * 1000) # Fallback nếu dữ liệu rỗng
    try:
        # Phân tích cú pháp chuỗi ISO 8601
        dt_obj = parser.isoparse(iso_str)
        # Chuyển đổi thành epoch milliseconds
        return int(dt_obj.timestamp() * 1000)
    except (ValueError, TypeError):
        # Fallback nếu định dạng không hợp lệ
        return int(datetime.utcnow().timestamp() * 1000)

try:
    df = pd.read_csv(path_data)
    print(f"Reading data from {path_data}...")

    # Sử dụng tqdm để tạo thanh tiến trình
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Sending to Kafka"):
        record = row.to_dict()
        record = {k: None if pd.isna(v) else v for k, v in record.items()}

        # === FIX CORE LOGIC HERE ===
        # Chuyển đổi timestamp_iso gốc thay vì ghi đè nó
        original_iso_timestamp = record.get("timestamp_iso")
        record["timestamp_iso"] = iso_to_epoch_ms(original_iso_timestamp)
        # ===========================

        message_key = record.get('list_id')
        producer.send('raw_job_postings', key=message_key, value=record)

    producer.flush()
    print("\nAll data has been sent to Kafka topic 'raw_job_postings'!")

except FileNotFoundError:
    print(f"Error: Data file not found at {path_data}")
    print("Please run 'make crawl' first.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    print("Closing Kafka producer...")
    producer.close()