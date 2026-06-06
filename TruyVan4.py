from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Truy_van_4")\
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

#Truy vấn 4
Truy_van_4 = """
WITH customer_spending AS
(
   SELECT
       c.customer_unique_id,
       SUM(p.payment_value) AS total_spending
   FROM customers c
   JOIN orders o
       ON c.customer_id = o.customer_id
   JOIN payments p
       ON o.order_id = p.order_id
   GROUP BY
       c.customer_unique_id
)
SELECT *
FROM customer_spending
WHERE total_spending >
(
   SELECT AVG(total_spending)
   FROM customer_spending
)
"""
ket_qua_4 = spark.sql(Truy_van_4)
ket_qua_4.show(100, False)

#Trực quan hóa dữ liệu truy vấn 4
import matplotlib.pyplot as plt
df4 = ket_qua_4.toPandas()
df4 = df4.sort_values(
    by="total_spending",
    ascending=False
).head(20)
plt.figure(figsize=(12,6))
plt.bar(
    df4["customer_unique_id"],
    df4["total_spending"]
)
plt.xticks(rotation=90)
plt.title(
    "Top khách hàng chi tiêu cao nhất"
)
plt.tight_layout()
plt.show()

