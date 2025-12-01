#!/usr/bin/env python3
import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
import os

BROKER = "172.22.165.97:9092"
TOPICS = ["topic-net", "topic-disk"]

NET_FILE = "/home/khushimahesh/output/net_data.csv"
DISK_FILE = "/home/khushimahesh/output/disk_data.csv"
BATCH_SIZE = 5000

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="consumer2-net-disk-v2",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
print(f"✅ Connected to Kafka broker at {BROKER}") 
print(f"Listening for messages on topics: {TOPICS}")

records_net = []
records_disk = []

try:
    for msg in consumer:
        data = msg.value
        if data['metric'] == 'net':
            records_net.append(data)
        else:
            records_disk.append(data)

        if len(records_net) >= BATCH_SIZE:
            pd.DataFrame(records_net).to_csv(
                NET_FILE, mode='a', index=False,
                header=not os.path.exists(NET_FILE)
            )
            records_net.clear()

        if len(records_disk) >= BATCH_SIZE:
            pd.DataFrame(records_disk).to_csv(
                DISK_FILE, mode='a', index=False,
                header=not os.path.exists(DISK_FILE)
            )
            records_disk.clear()

except KeyboardInterrupt:
    print("Interrupted by user.")

finally:
    if records_net:
        pd.DataFrame(records_net).to_csv(
            NET_FILE, mode='a', index=False,
            header=not os.path.exists(NET_FILE)
        )
    if records_disk:
        pd.DataFrame(records_disk).to_csv(
            DISK_FILE, mode='a', index=False,
            header=not os.path.exists(DISK_FILE)
        )
    consumer.close()
    print("✅ Consumer closed. NET and DISK files written.")

