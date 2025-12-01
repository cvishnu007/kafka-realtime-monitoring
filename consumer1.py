#!/usr/bin/env python3
import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
import os

# ---------- CONFIG ----------
BROKER = "172.22.165.97:9092"
TOPICS = ["topic-cpu", "topic-mem"]

CPU_FILE = "/home/hadoop/output/cpu_data.csv"
MEM_FILE = "/home/hadoop/output/mem_data.csv"
BATCH_SIZE = 5000
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="consumer1-cpu-mem-v2",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"✅ Connected to Kafka broker at {BROKER}") 
print(f"Listening for messages on topics: {TOPICS}")

records_cpu, records_mem = [], []

try:
    for msg in consumer:
        data = msg.value
        metric = data.get("metric")
        server_id = data.get("server_id")
        ts = data.get("timestamp") 
        value = data.get("value")

        if metric == "cpu":
            records_cpu.append({
                "ts": ts,
                "server_id": server_id,
                "cpu_pct": value
            })
        elif metric == "mem":
            records_mem.append({
                "ts": ts,
                "server_id": server_id,
                "mem_pct": value
            })

        if len(records_cpu) >= BATCH_SIZE:
            pd.DataFrame(records_cpu).to_csv(
                CPU_FILE, mode='a', index=False,
                header=not os.path.exists(CPU_FILE)
            )
            print(f"💾 Wrote {BATCH_SIZE} CPU rows to {CPU_FILE}")
            records_cpu.clear()

        # Batch write for MEM
        if len(records_mem) >= BATCH_SIZE:
            pd.DataFrame(records_mem).to_csv(
                MEM_FILE, mode='a', index=False,
                header=not os.path.exists(MEM_FILE)
            )
            print(f"💾 Wrote {BATCH_SIZE} MEM rows to {MEM_FILE}")
            records_mem.clear()

except KeyboardInterrupt:
    print("\n🛑 Interrupted by user.")

finally:
    
    if records_cpu:
        pd.DataFrame(records_cpu).to_csv(
            CPU_FILE, mode='a', index=False,
            header=not os.path.exists(CPU_FILE)
        )
    if records_mem:
        pd.DataFrame(records_mem).to_csv(
            MEM_FILE, mode='a', index=False,
            header=not os.path.exists(MEM_FILE)
        )
    consumer.close()
    print("✅ Consumer closed. Final data written to CSVs.")

