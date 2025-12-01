#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, sys, shutil, glob

# ===============================
# ENVIRONMENT FIXES
# ===============================
os.environ["SPARK_LOCAL_IP"] = "192.168.191.134"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-arm64"

spark = (
    SparkSession.builder
    .appName("NetworkDiskAnalysis")
    .master("local[*]")
    .config("spark.driver.bindAddress", "192.168.191.134")
    .config("spark.driver.host", "192.168.191.134")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

# ===============================
# CONFIG
# ===============================
NET_THRESHOLD = 4776.43
DISK_THRESHOLD = 4881.54

net_csv = "/home/khushimahesh/output/net_data.csv"
disk_csv = "/home/khushimahesh/output/disk_data.csv"
out_csv = "/home/khushimahesh/output/team_136_NET_DISK.csv"
temp_dir = "/home/khushimahesh/spark_jobs/temp_out"

window_size = 30  # seconds
slide = 10        # seconds

# ===============================
# LOAD & NORMALIZE DATA
# ===============================
try:
    df_net = (
        spark.read.option("header", True).csv(net_csv)
        .withColumnRenamed("value", "net_in")
        .withColumn("net_in", F.col("net_in").cast("double"))
        .drop("metric")
        .withColumn("ts_fixed", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ts_sec", F.col("ts_fixed").cast("long"))
        .dropDuplicates(["timestamp", "server_id"])
        .select("server_id", "ts_sec", "net_in")
    )

    df_disk = (
        spark.read.option("header", True).csv(disk_csv)
        .withColumnRenamed("value", "disk_io")
        .withColumn("disk_io", F.col("disk_io").cast("double"))
        .drop("metric")
        .withColumn("ts_fixed", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ts_sec", F.col("ts_fixed").cast("long"))
        .dropDuplicates(["timestamp", "server_id"])
        .select("server_id", "ts_sec", "disk_io")
    )

    df = (
        df_net.alias("n")
        .join(
            df_disk.alias("d"),
            on=[
                F.col("n.server_id") == F.col("d.server_id"),
                F.col("n.ts_sec") == F.col("d.ts_sec"),
            ],
            how="full_outer",
        )
        .select(
            F.coalesce(F.col("n.server_id"), F.col("d.server_id")).alias("server_id"),
            F.coalesce(F.col("n.ts_sec"), F.col("d.ts_sec")).alias("ts_sec"),
            F.col("n.net_in").alias("net_in"),
            F.col("d.disk_io").alias("disk_io"),
        )
    )
except Exception as e:
    print(f"❌ Error loading data: {e}")
    spark.stop()
    sys.exit(1)

if df.rdd.isEmpty():
    print("❌ No rows found after loading.")
    spark.stop()
    sys.exit(1)

# ===============================
# BUILD ALIGNED SLIDING WINDOWS
# ===============================
min_ts = df.agg(F.min("ts_sec")).first()[0]
max_ts = df.agg(F.max("ts_sec")).first()[0]

aligned_start = (min_ts // slide) * slide
aligned_end_inclusive = (max_ts // slide) * slide

starts = list(range(aligned_start, aligned_end_inclusive + 1, slide))
windows = [(s, s + window_size) for s in starts]

spark_windows = spark.createDataFrame(windows, ["win_start", "win_end"])
servers = df.select("server_id").distinct()

print(f"📊 Data range (aligned): {aligned_start} to {aligned_end_inclusive} ({aligned_end_inclusive - aligned_start} seconds)")
print(f"📊 Number of servers: {servers.count()}")
print(f"📊 Number of windows: {len(windows)}")
print(f"📊 Expected rows: {servers.count() * len(windows)}")

servers_cross = servers.crossJoin(spark_windows).alias("sw")
events = df.alias("e")

# ===============================
# RANGE JOIN & AGGREGATION
# ===============================
joined = (
    servers_cross.join(
        events,
        on=[
            F.col("sw.server_id") == F.col("e.server_id"),
            (F.col("e.ts_sec") >= F.col("sw.win_start")) & (F.col("e.ts_sec") < F.col("sw.win_end")),
        ],
        how="left",
    )
    .select(
        F.col("sw.server_id").alias("server_id"),
        F.col("sw.win_start").alias("win_start"),
        F.col("sw.win_end").alias("win_end"),
        F.col("e.net_in").alias("net_in"),
        F.col("e.disk_io").alias("disk_io"),
    )
)

agg = (
    joined.groupBy("server_id", "win_start", "win_end")
    .agg(
        F.max("net_in").alias("max_net_in"),
        F.max("disk_io").alias("max_disk_io"),
    )
    .withColumn("max_net_in", F.coalesce(F.col("max_net_in"), F.lit(0.0)))
    .withColumn("max_disk_io", F.coalesce(F.col("max_disk_io"), F.lit(0.0)))
)

# ===============================
# ALERTS, FORMATTING, ORDER
# ===============================
with_alerts = (
    agg.withColumn(
        "alert",
        F.when(
            (F.col("max_net_in") > NET_THRESHOLD) & (F.col("max_disk_io") > DISK_THRESHOLD),
            F.lit("Network flood + Disk thrash suspected"),
        )
        .when(F.col("max_net_in") > NET_THRESHOLD, F.lit("Possible DDoS"))
        .when(F.col("max_disk_io") > DISK_THRESHOLD, F.lit("Disk thrash suspected"))
    #    .otherwise(F.lit("Normal")),
    )
    .withColumn("window_start", F.from_unixtime("win_start", "HH:mm:ss"))
    .withColumn("window_end", F.from_unixtime("win_end", "HH:mm:ss"))
)

ordered = with_alerts.orderBy("server_id", "win_start")

result = ordered.select(
    "server_id",
    "window_start",
    "window_end",
    F.round("max_net_in", 2).alias("max_net_in"),
    F.round("max_disk_io", 2).alias("max_disk_io"),
    "alert",
)

final_count = result.count()
print(f"📊 Final row count: {final_count}")
print("\n📋 First 10 rows:")
result.show(10, truncate=False)

# ===============================
# SAVE OUTPUT (SINGLE FILE)
# ===============================
try:
    os.makedirs(temp_dir, exist_ok=True)
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)

    out_file = glob.glob(f"{temp_dir}/part-*.csv")[0]
    if os.path.exists(out_csv):
        os.remove(out_csv)
    shutil.move(out_file, out_csv)
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"✅ Job completed successfully! Output: {out_csv}")
    print(f"✅ Rows written: {final_count}")
finally:
    spark.stop()

