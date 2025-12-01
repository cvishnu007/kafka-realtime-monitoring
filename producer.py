#!/usr/bin/env python3
# producer_instant.py — Send all 28,800 rows instantly to Kafka

import json
import pandas as pd
from kafka import KafkaProducer
from datetime import date

# === CONFIGURATION ===
BROKER = "172.22.165.97:9092"  # Kafka broker ZeroTier IP
DATA_CSV = "/home/vboxuser/Downloads/dataset.csv"

# === INITIALIZE PRODUCER ===
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    batch_size=65536,       # 64 KB batches
    compression_type='gzip' # better throughput
)

# === READ DATASET ===
df = pd.read_csv(DATA_CSV)
today = date.today().isoformat()
total = len(df)

print(f"🚀 Sending all {total} rows instantly to Kafka broker {BROKER}...")

# === STREAM ALL ROWS ===
for i, row in enumerate(df.itertuples(index=False), start=1):
    ts = str(row.ts).strip()
    timestamp = f"{today} {ts}"
    server_id = row.server_id

    messages = [
        ("topic-cpu", {"timestamp": timestamp, "server_id": server_id, "metric": "cpu", "value": float(row.cpu_pct)}),
        ("topic-mem", {"timestamp": timestamp, "server_id": server_id, "metric": "mem", "value": float(row.mem_pct)}),
        ("topic-net", {"timestamp": timestamp, "server_id": server_id, "metric": "net", "value": float(row.net_in)}),
        ("topic-disk", {"timestamp": timestamp, "server_id": server_id, "metric": "disk", "value": float(row.disk_io)})
    ]

    for topic, msg in messages:
        producer.send(topic, msg)

    if i % 1000 == 0:
        print(f"✅ Sent {i}/{total} rows...")

producer.flush()
producer.close()

print(f"🎯 Finished sending all {total} rows to Kafka successfully!")
