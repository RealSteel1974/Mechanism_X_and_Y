from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3
import psycopg2
import time
import datetime
import uuid
import numpy as np
from builtins import max  


# --------------------------
# Configuration
# --------------------------
S3_INPUT_PATH = "s3a://onkarpawar-take-home-assg/transactions_chunks_databricks/"
S3_OUTPUT_PATH = "s3a://onkarpawar-take-home-assg/transactions_detections_databricks/"
JDBC_URL = "jdbc:postgresql://databrickspgdb.csnii8c0ufz7.us-east-1.rds.amazonaws.com:5432/mechanism"
JDBC_PROPS = {"user": "postgres", "password": "############", "driver": "org.postgresql.Driver"}

AWS_ACCESS_KEY = "ACCESS_KEY"
AWS_SECRET_KEY = "SECRET_KEY"
AWS_REGION = "us-east-1"
S3_BUCKET = "onkarpawar-take-home-assg"
S3_PREFIX = "transactions_chunks_databricks/"

# --------------------------
# Initialize Spark
# --------------------------
spark = SparkSession.builder \
    .appName("Mechanism Y") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------
# PostgreSQL connection
# --------------------------
def get_pg_connection():
    return psycopg2.connect(
        host="databrickspgdb.csnii8c0ufz7.us-east-1.rds.amazonaws.com",
        port=5432,
        database="mechanism",
        user="postgres",
        password="CdacMumbai25"
    )

# --------------------------
# Process Helpers
# --------------------------
def update_summary_tables(df):
    merchant_agg = df.groupBy("merchant").count().toPandas()
    cust_merch_agg = df.groupBy("customer", "merchant").agg(count("*").alias("txn_count"), sum("amount").alias("sum_amount")).toPandas()
    gender_agg = df.groupBy("merchant", "gender").count().toPandas()

    conn = get_pg_connection()
    cur = conn.cursor()

    for _, row in merchant_agg.iterrows():
        cur.execute(
            "INSERT INTO merchant_transaction_counts (merchant_id, total_txns) VALUES (%s, %s) "
            "ON CONFLICT (merchant_id) DO UPDATE SET total_txns = merchant_transaction_counts.total_txns + %s;",
            (row['merchant'], int(row['count']), int(row['count']))
        )

    for _, row in cust_merch_agg.iterrows():
        cur.execute(
            "INSERT INTO customer_merchant_summary (customer_id, merchant_id, txn_count, total_amount) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (customer_id, merchant_id) DO UPDATE SET txn_count = customer_merchant_summary.txn_count + %s, total_amount = customer_merchant_summary.total_amount + %s;",
            (row['customer'], row['merchant'], int(row['txn_count']), float(row['sum_amount']),
             int(row['txn_count']), float(row['sum_amount']))
        )

    for _, row in gender_agg.iterrows():
        cur.execute(
            "INSERT INTO merchant_gender_summary (merchant_id, gender, gender_txn_count) VALUES (%s, %s, %s) "
            "ON CONFLICT (merchant_id, gender) DO UPDATE SET gender_txn_count = merchant_gender_summary.gender_txn_count + %s;",
            (row['merchant'], row['gender'], int(row['count']), int(row['count']))
        )

    conn.commit()
    cur.close()
    conn.close()

def detect_patterns(df_chunk, customer_importance_df):
    now = datetime.datetime.now().astimezone().isoformat()
    detections = []

    merchants_in_chunk = [row['merchant'] for row in df_chunk.select("merchant").distinct().collect()]

    for merchant_id in merchants_in_chunk:
        merchant_df = spark.read.jdbc(JDBC_URL, "merchant_transaction_counts", properties=JDBC_PROPS) \
            .filter(col("merchant_id") == merchant_id)
        total_txn = merchant_df.collect()[0]['total_txns'] if merchant_df.count() > 0 else 0
        if total_txn < 50000:
            continue

        cm_summary = spark.read.jdbc(JDBC_URL, "customer_merchant_summary", properties=JDBC_PROPS) \
            .filter(col("merchant_id") == merchant_id)

        cust_w = cm_summary.join(customer_importance_df,
                                  (cm_summary.customer_id == customer_importance_df.Source) &
                                  (cm_summary.merchant_id == customer_importance_df.Target),
                                  "inner").select(cm_summary.customer_id.alias("customer"), customer_importance_df.Weight)

        if cust_w.count() == 0:
            continue

        weights_list = cust_w.select("Weight").rdd.map(lambda r: r[0]).collect()
        perc_1 = float(np.percentile(weights_list, 1))

        low_weight_customers = cust_w.filter(col("Weight") <= perc_1).select("customer").distinct()
        low_cust_counts = cm_summary.join(low_weight_customers, cm_summary.customer_id == low_weight_customers.customer)
        txn_counts = low_cust_counts.select(col("customer_id"), col("txn_count")).orderBy(col("txn_count").desc())

        if txn_counts.count() == 0:
            continue

        top_limit = max(int(txn_counts.count() * 0.01), 1)
        top_customers = txn_counts.limit(top_limit)

        for row in top_customers.collect():
            detections.append(("PatId1", "UPGRADE", row['customer_id'], merchant_id))

    cm_summary_all = spark.read.jdbc(JDBC_URL, "customer_merchant_summary", properties=JDBC_PROPS)
    child_df = cm_summary_all.filter((col("txn_count") >= 80) & (col("total_amount")/col("txn_count") < 23))
    for row in child_df.select("customer_id", "merchant_id").collect():
        detections.append(("PatId2", "CHILD", row['customer_id'], row['merchant_id']))

    gender_summary_all = spark.read.jdbc(JDBC_URL, "merchant_gender_summary", properties=JDBC_PROPS)
    pivot = gender_summary_all.groupBy("merchant_id").pivot("gender", ["F", "M"]).sum("gender_txn_count").na.fill(0)
    dei_df = pivot.filter((col("F") < col("M")) & (col("F") > 100))
    for row in dei_df.select("merchant_id").collect():
        detections.append(("PatId3", "DEI-NEEDED", "", row['merchant_id']))

    return detections

def is_chunk_processed(file_key):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_chunks WHERE chunk_key = %s;", (file_key,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

def mark_chunk_processed(file_key):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO processed_chunks (chunk_key) VALUES (%s) ON CONFLICT DO NOTHING;", (file_key,))
    conn.commit()
    cur.close()
    conn.close()

def process_new_chunks():
    s3 = boto3.client('s3', region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)

    while True:
        print("Checking for new chunks...")
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        if "Contents" not in resp:
            time.sleep(5)
            continue

        new_files = sorted([obj['Key'] for obj in resp['Contents'] if obj['Key'].endswith(".csv")])

        for file_key in new_files:
            if is_chunk_processed(file_key):
                continue

            full_path = f"s3a://{S3_BUCKET}/{file_key}"
            print(f"Processing: {full_path}")
            df = spark.read.option("header", True).csv(full_path)
            df = df.withColumn("amount", col("amount").cast("double"))
            df = df.withColumn("gender", regexp_replace("gender", "'", ""))

            update_summary_tables(df)

            try:
                customer_importance_df = spark.read.option("header", True).csv("s3a://onkarpawar-take-home-assg/CustomerImportance.csv")
                customer_importance_df = customer_importance_df.withColumn("Weight", col("Weight").cast("double"))
            except:
                customer_importance_df = spark.createDataFrame([], schema="Source STRING, Target STRING, Weight DOUBLE, typeTrans STRING, fraud STRING")

            detections = detect_patterns(df, customer_importance_df)

            for i in range(0, len(detections), 50):
                part = detections[i:i+50]
                if not part:
                    continue

                output = spark.createDataFrame([
                    (datetime.datetime.now().astimezone().isoformat(), datetime.datetime.now().astimezone().isoformat(), pid, action, cust, merch)
                    for pid, action, cust, merch in part
                ], ["YStartTime", "detectionTime", "patternId", "ActionType", "customerName", "MerchantId"])

                out_filename = f"detection_{uuid.uuid4().hex}.csv"
                #output.coalesce(1).write.option("header", True).csv(f"{S3_OUTPUT_PATH}/{out_filename}")
                output.write.mode("overwrite").parquet(f"{S3_OUTPUT_PATH}/{out_filename}")

            mark_chunk_processed(file_key)

        time.sleep(5)

if __name__ == "__main__":
    process_new_chunks()
