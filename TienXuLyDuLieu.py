from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("Olist Big Data")\
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
#Đánh giá cấu trúc dữ liệu
customers.printSchema()
orders.printSchema()
order_items.printSchema()
payments.printSchema()
reviews.printSchema()
products.printSchema()
sellers.printSchema()
geolocation.printSchema()
translation.printSchema()

#Kiểm tra giá trị bị thiếu
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
