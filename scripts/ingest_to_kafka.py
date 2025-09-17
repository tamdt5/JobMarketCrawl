from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime
from tqdm import tqdm

path_data = '../data/jobs.csv'

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

try:
    df = pd.read_csv(path_data)
    print(f"Reading data from {path_data}...")
    
    # Sử dụng tqdm để tạo thanh tiến trình
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Sending to Kafka"):
        record = row.to_dict()
        record = {k: None if pd.isna(v) else v for k, v in record.items()}
        record["timestamp_iso"] = int(datetime.utcnow().timestamp() * 1000)
        
        # Lấy list_id làm key cho message
        message_key = record.get('list_id')
        
        producer.send('raw_job_postings', key=message_key, value=record)
    
    producer.flush()
    print("\nAll data has been sent to Kafka topic 'raw_job_postings'!")

except FileNotFoundError:
    print(f"Error: Data file not found at {path_data}")
    print("Please run 'make crawl' first.")
except Exception as e:
    print(f"An error occurred: {e}")