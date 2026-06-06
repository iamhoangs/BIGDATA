from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Query_6")\
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

 #Truy vấn 6
Truy_van_6 = """
WITH quarterly_revenue AS
(
    SELECT
        YEAR(order_purchase_timestamp) AS year,
        QUARTER(order_purchase_timestamp) AS quarter,
        SUM(payment_value) AS revenue
    FROM orders o
    JOIN payments p
        ON o.order_id = p.order_id
    GROUP BY
        YEAR(order_purchase_timestamp),
        QUARTER(order_purchase_timestamp)
)
SELECT
    year,
    quarter,
    revenue,
    revenue -
    LAG(revenue)
    OVER(
        ORDER BY year, quarter
    ) AS growth
FROM quarterly_revenue
"""
ket_qua = spark.sql(Truy_van_6)
ket_qua.show(100, False)

#Trực quan hóa dữ liệu truy vấn 6
import matplotlib.pyplot as plt
df6 = ket_qua.toPandas()

plt.figure(figsize=(12,6))

plt.plot(
range(len(df6)),
df6["revenue"],
marker="o"
)
plt.title(
"Doanh thu theo quý"
)
plt.tight_layout()
plt.show()
