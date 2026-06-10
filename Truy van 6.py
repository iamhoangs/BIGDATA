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

 #Truy vấn 6
Truy_van_6 = """
WITH quarterly_revenue AS
(
    SELECT
        YEAR(o.order_purchase_timestamp) AS year,
        QUARTER(o.order_purchase_timestamp) AS quarter,
        ROUND(SUM(p.payment_value), 2) AS revenue
    FROM orders o
    JOIN payments p
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        YEAR(o.order_purchase_timestamp),
        QUARTER(o.order_purchase_timestamp)
),

growth_table AS
(
    SELECT
        year,
        quarter,
        revenue,
        LAG(revenue) OVER (
            ORDER BY year, quarter
        ) AS previous_revenue
    FROM quarterly_revenue
)

SELECT
    year,
    quarter,
    revenue,
    previous_revenue,
    ROUND(
        revenue - previous_revenue,
        2
    ) AS growth,
    ROUND(
        (revenue - previous_revenue) * 100
        / previous_revenue,
        2
    ) AS growth_rate_percent
FROM growth_table
ORDER BY year, quarter
"""

ket_qua = spark.sql(Truy_van_6)

ket_qua.show(100, False)

# Trực quan hóa dữ liệu
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
df6 = ket_qua.toPandas()
df6["period"] = (
    df6["year"].astype(str)
    + "-Q"
    + df6["quarter"].astype(str)
)
plt.figure(figsize=(14,6))
plt.plot(
    df6["period"],
    df6["revenue"],
    marker="o",
    linewidth=2
)
plt.title(
    "Doanh thu theo quý giai đoạn 2016 - 2018"
)
plt.xlabel("Quý")
plt.ylabel("Doanh thu (Triệu BRL)")
plt.gca().yaxis.set_major_formatter(
    FuncFormatter(lambda x, p: f'{x/1000000:.1f}M')
)
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()
