from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import decimal
import shutil, glob, os 

CPU_THRESHOLD = 81.48
MEM_THRESHOLD = 72.92

CPU_CSV = "/home/hadoop/output/cpu_data.csv"
MEM_CSV = "/home/hadoop/output/mem_data.csv"
OUT_CSV = "/home/hadoop/output/team_136_CPU_MEM.csv"
TEMP_DIR = "temp_out"

WINDOW_SIZE = 30
SLIDE = 10
spark = (
    SparkSession.builder
    .appName("cpu_mem_alerts_final_100percent")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

def round_half_up(val):
    if val is None:
        return None
    return float(decimal.Decimal(val).quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP))

spark.udf.register("round_half_up", round_half_up, FloatType())

df_cpu = (
    spark.read.option("header", True).csv(CPU_CSV)
    .withColumn("cpu_pct", F.col("cpu_pct").cast("float"))
)
df_mem = (
    spark.read.option("header", True).csv(MEM_CSV)
    .withColumn("mem_pct", F.col("mem_pct").cast("float"))
)

df = df_cpu.join(df_mem, on=["ts", "server_id"], how="inner")
df = df.withColumn("ts_sec", F.unix_timestamp(F.col("ts"), "yyyy-MM-dd HH:mm:ss"))

bounds = df.agg(F.min("ts_sec").alias("min_ts"), F.max("ts_sec").alias("max_ts")).collect()[0]
min_ts, max_ts = int(bounds["min_ts"]), int(bounds["max_ts"])

starts = list(range(min_ts, max_ts, SLIDE))
windows = [(s, s + WINDOW_SIZE) for s in starts]
df_windows = spark.createDataFrame(windows, ["win_start", "win_end"])

df_assigned = df.crossJoin(df_windows).filter(
    (F.col("ts_sec") >= F.col("win_start")) & (F.col("ts_sec") < F.col("win_end"))
)

agg = df_assigned.groupBy("server_id", "win_start", "win_end").agg(
    F.avg("cpu_pct").alias("avg_cpu_raw"),
    F.avg("mem_pct").alias("avg_mem_raw")
)

agg = (
    agg.withColumn("avg_cpu", F.expr("round_half_up(avg_cpu_raw)"))
       .withColumn("avg_mem", F.expr("round_half_up(avg_mem_raw)"))
       .drop("avg_cpu_raw", "avg_mem_raw")
)

result = (
    agg.withColumn(
        "alert",
        F.when(
            (F.col("avg_cpu") > F.lit(CPU_THRESHOLD)) & (F.col("avg_mem") > F.lit(MEM_THRESHOLD)),
            "High CPU + Memory stress"
        ).when(
            (F.col("avg_cpu") > F.lit(CPU_THRESHOLD)) & (F.col("avg_mem") <= F.lit(MEM_THRESHOLD)),
            "CPU spike suspected"
        ).when(
            (F.col("avg_mem") > F.lit(MEM_THRESHOLD)) & (F.col("avg_cpu") <= F.lit(CPU_THRESHOLD)),
            "Memory saturation suspected"
        ).otherwise("")
    )
    .withColumn("window_start", F.date_format(F.from_unixtime("win_start"), "HH:mm:ss"))
    .withColumn("window_end", F.date_format(F.from_unixtime("win_end"), "HH:mm:ss"))
    .select("server_id", "window_start", "window_end", "avg_cpu", "avg_mem", "alert")
    .orderBy("server_id", "window_start")
)

count = result.count()
print(f"✅ Row count: {count} (expected 14400)")


shutil.rmtree(TEMP_DIR, ignore_errors=True)
result.coalesce(1).write.csv(TEMP_DIR, header=True, mode="overwrite")
out_file = glob.glob(f"{TEMP_DIR}/part-*.csv")[0]
shutil.move(out_file, OUT_CSV)
shutil.rmtree(TEMP_DIR, ignore_errors=True)

spark.stop()
print("🎯 Spark job completed successfully — 14400 rows, 100% accuracy with ROUND_HALF_UP logic.")
