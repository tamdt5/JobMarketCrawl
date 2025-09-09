from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime
import numpy as np  # Thêm import này để xử lý NaN

path_data = './jobs.csv'

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv(path_data)
for index, row in df.iterrows():
    record = row.to_dict()
    record = {k: None if pd.isna(v) else v for k, v in record.items()}
    record["timestamp_iso"] = int(datetime.utcnow().timestamp() * 1000)
    print(f"Sending row {index}: {record}")
    producer.send('crawl_res', record)
producer.flush()
print("Dữ liệu đã đẩy vào Kafka!")