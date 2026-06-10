from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, sum as _sum, count, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

logging.basicConfig(level=logging.INFO)

try:
    logging.info("Tiến hành khởi tạo kết nối Spark Streaming.")

    spark = SparkSession.builder \
        .appName("Demo Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.jars", "D:\\DEMO_BIGDATA\\mssql-jdbc-12.2.0.jre8.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "orders_topic") \
        .option("startingOffsets", "latest") \
        .load()

    logging.info("Kết nối Kafka thành công, bắt đầu bóc tách cấu trúc dữ liệu JSON.")

    olist_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_value", DoubleType(), True),
        StructField("customer_city", StringType(), True)
    ])

    kafka_json_df = df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = kafka_json_df.withColumn("data", from_json(col("json_str"), olist_schema)).select("data.*")

    streaming_df = parsed_df.withColumn("processing_time", current_timestamp())

    logging.info("Kích hoạt luồng ghi dữ liệu chi tiết dạng Parquet xuống Hadoop HDFS.")
    query_hdfs = parsed_df.writeStream \
        .format("json") \
        .option("path", "hdfs://localhost:9000/stream/orders") \
        .option("checkpointLocation", "hdfs://localhost:9000/stream/checkpoint") \
        .outputMode("append") \
        .start()

    metrics_df = streaming_df \
        .groupBy(
        window(col("processing_time"), "10 seconds"),
        col("product_category_name"),
        col("customer_city"),
        col("payment_type")
    ) \
        .agg(
        count("order_id").alias("total_orders"),
        _sum("payment_value").alias("total_revenue"),
        collect_list("order_id").alias("order_ids")
    ) \
        .select(
        col("window.end").alias("Thoi_Gian_Dat_Hang"),
        col("order_ids").alias("Ma_Don_Hang"),
        col("product_category_name").alias("San_Pham"),
        col("customer_city").alias("Thanh_Pho"),
        col("payment_type").alias("Phuong_Thuc_Thanh_Toan"),
        col("total_orders").alias("So_Luong"),
        col("total_revenue").alias("Tong_Doanh_Thu")
    )

    logging.info("Kích hoạt bảng hiển thị Console Real-time.")
    query_console = metrics_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()


    logging.info("Hệ thống xử lý Spark Streaming đã vận hành!")

    query_hdfs.awaitTermination()
    query_console.awaitTermination()

except Exception as e:
    logging.error(f"Xảy ra lỗi trong quá trình xử lý: {e}")