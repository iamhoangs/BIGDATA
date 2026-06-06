from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Query_9")\
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

 #Truy vấn 9
Truy_van_9 = """
WITH seller_performance AS
(
    SELECT
        oi.seller_id,
        SUM(oi.price) AS revenue,
        AVG(r.review_score) AS avg_review
    FROM order_items oi
    JOIN reviews r
        ON oi.order_id = r.order_id
    GROUP BY oi.seller_id
)
SELECT *
FROM seller_performance
WHERE revenue >
(
    SELECT AVG(revenue)
    FROM seller_performance
)
AND avg_review < 4
"""
ket_qua = spark.sql(Truy_van_9)
ket_qua.show(100, False)

#Trực quan hóa dữ liệu truy vấn 9
import matplotlib.pyplot as plt
df9 = ket_qua.toPandas()
plt.figure(figsize=(10,6))
plt.scatter(
    df9["revenue"],
    df9["avg_review"]
)
plt.title(
    "Mối quan hệ giữa doanh thu và đánh giá Seller"
)
plt.xlabel("Doanh thu")
plt.ylabel("Điểm đánh giá")
plt.tight_layout()
plt.show()
