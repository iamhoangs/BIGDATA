from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Truy_van_3")\
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

#Truy vấn 3
Truy_van_3 = """
WITH seller_revenue AS
(
   SELECT
       s.seller_state,
       oi.seller_id,
       SUM(oi.price) AS revenue
   FROM order_items oi
   JOIN sellers s
       ON oi.seller_id = s.seller_id
   GROUP BY
       s.seller_state,
       oi.seller_id
)
SELECT *
FROM
(
   SELECT
       seller_state,
       seller_id,
       revenue,
       ROW_NUMBER()
       OVER(
           PARTITION BY seller_state
           ORDER BY revenue DESC
       ) AS ranking
   FROM seller_revenue
) t
WHERE ranking <= 3
"""
ket_qua_3 = spark.sql(Truy_van_3)
ket_qua_3.show(100, False)

#Trực quan hóa dữ liệu truy vấn 3
import matplotlib.pyplot as plt
df3 = ket_qua_3.toPandas()
plt.figure(figsize=(12,6))
plt.bar(
    df3["seller_state"],
    df3["revenue"]
)
plt.title(
    "Top Seller theo bang"
)
plt.tight_layout()
plt.show()

