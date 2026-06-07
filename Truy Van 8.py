from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Query_8")\
    .getOrCreate()
customers = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)
geolocation = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_geolocation_dataset.csv",
    header=True,
    inferSchema=True
)
order_items = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)
payments = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)
reviews = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True
)
orders = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)
products = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)
sellers = spark.read.csv(
    "hdfs://localhost:9000/Olist/olist_sellers_dataset.csv",
    header=True,
    inferSchema=True
)
translation = spark.read.csv(
    "hdfs://localhost:9000/Olist/product_category_name_translation.csv",
    header=True,
    inferSchema=True
)#Kiểm tra giá trị bị thiếu
from pyspark.sql.functions import *
datasets = {
    "customers": customers,
    "geolocation": geolocation,
    "order_items": order_items,
    "payments": payments,
    "reviews": reviews,
    "orders": orders,
    "products": products,
    "sellers": sellers,
    "translation": translation
}
for name, df in datasets.items():
    print(f"\n Kết quả kiểm tra giá trị bị thiếu của bảng: {name.upper()}")
    df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]).show()

#Kiểm tra dữ liệu trùng lặp
datasets = {
    "customers": customers,
    "geolocation": geolocation,
    "order_items": order_items,
    "payments": payments,
    "reviews": reviews,
    "orders": orders,
    "products": products,
    "sellers": sellers,
    "translation": translation
}
# Loại bỏ dữ liệu trùng lặp
for name, df in datasets.items():

    before = df.count()

    after = df.dropDuplicates().count()

    print(
        f"{name}: Before = {before}, After = {after}"
    )

#Chuẩn hóa dữ liệu thời gian
from pyspark.sql.functions import to_timestamp

orders = orders \
.withColumn(
    "order_purchase_timestamp",
    to_timestamp("order_purchase_timestamp")
) \
.withColumn(
    "order_approved_at",
    to_timestamp("order_approved_at")
) \
.withColumn(
    "order_delivered_customer_date",
    to_timestamp("order_delivered_customer_date")
) \
.withColumn(
    "order_estimated_delivery_date",
    to_timestamp("order_estimated_delivery_date")
)
customers.createOrReplaceTempView("customers")
geolocation.createOrReplaceTempView("geolocation")
order_items.createOrReplaceTempView("order_items")
payments.createOrReplaceTempView("payments")
reviews.createOrReplaceTempView("reviews")
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")
sellers.createOrReplaceTempView("sellers")
translation.createOrReplaceTempView("translation")

 #Truy vấn 8
Truy_van_8 = """
WITH delivery_review AS
(
    SELECT
        r.review_score,
        DATEDIFF(
            order_delivered_customer_date,
            order_purchase_timestamp
        ) AS delivery_days
    FROM orders o
    JOIN reviews r
        ON o.order_id = r.order_id
)
SELECT
    CASE
        WHEN delivery_days <= 5
            THEN 'Fast'
        WHEN delivery_days <= 10
            THEN 'Normal'
        ELSE 'Slow'
    END AS delivery_group,
    AVG(review_score) AS avg_review,
    COUNT(*) AS total_orders
FROM delivery_review
GROUP BY
    CASE
        WHEN delivery_days <= 5
            THEN 'Fast'
        WHEN delivery_days <= 10
            THEN 'Normal'
        ELSE 'Slow'
    END
"""
ket_qua = spark.sql(Truy_van_8)
ket_qua.show(100, False)

#Trực quan hóa dữ liệu truy vấn 8
import matplotlib.pyplot as plt
df8 = ket_qua.toPandas()
plt.figure(figsize=(8,6))
plt.bar(
    df8["delivery_group"],
    df8["avg_review"]
)
plt.title(
    "Đánh giá trung bình theo thời gian giao hàng"
)
plt.tight_layout()
plt.show()
