from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Query_10")\
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

 #Truy vấn 10
Truy_van_10 = """
WITH monthly_product AS
(
    SELECT
        oi.product_id,
        DATE_FORMAT(
            o.order_purchase_timestamp,
            'yyyy-MM'
        ) AS month,
        SUM(oi.price) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY
        oi.product_id,
        DATE_FORMAT(
            o.order_purchase_timestamp,
            'yyyy-MM'
        )
)
SELECT
    product_id,
    month,
    revenue,
    revenue - LAG(revenue)
    OVER(
        PARTITION BY product_id
        ORDER BY month
    ) AS growth
FROM monthly_product
"""
ket_qua = spark.sql(Truy_van_10)
ket_qua.show(100, False)

#Trực quan hóa dữ liệu truy vấn 10
import matplotlib.pyplot as plt
df10 = ket_qua.toPandas()
top_products = (
    df10.groupby("product_id")["revenue"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .index
)
df10 = df10[
    df10["product_id"].isin(top_products)
]
plt.figure(figsize=(12,6))
for product in top_products:
    temp = df10[
        df10["product_id"] == product
    ]
    plt.plot(
        temp["month"],
        temp["revenue"],
        marker="o",
        label=product[:8]
    )
plt.legend()
plt.xticks(rotation=45)
plt.title(
    "Xu hướng doanh thu của Top 5 sản phẩm"
)
plt.tight_layout()
plt.show()
