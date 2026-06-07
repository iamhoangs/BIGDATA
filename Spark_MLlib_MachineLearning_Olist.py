from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler



# ============================================================
# 0. CẤU HÌNH CHUNG
# ============================================================
HDFS_BASE_PATH = "hdfs://localhost:9000/Olist"
OUTPUT_DIR = Path("outputs/chapter5_mllib")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RANDOM_SEED = 42
TRAIN_RATIO = 0.8
TEST_RATIO = 0.2


# ============================================================
# 1. KHỞI TẠO SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder
    .appName("Olist_Chapter5_Spark_MLlib")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.debug.maxToStringFields", "300")

print("===== CHƯƠNG 5 - SPARK MLLIB: MACHINE LEARNING =====")
print("Spark version:", spark.version)
print("HDFS_BASE_PATH:", HDFS_BASE_PATH)
print("OUTPUT_DIR:", OUTPUT_DIR.resolve())


# ============================================================
# 2. NẠP DỮ LIỆU TỪ HDFS
# ============================================================
customers = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

orders = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

order_items = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

payments = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)

reviews = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True
)

products = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

sellers = spark.read.csv(
    f"{HDFS_BASE_PATH}/olist_sellers_dataset.csv",
    header=True,
    inferSchema=True
)

translation = spark.read.csv(
    f"{HDFS_BASE_PATH}/product_category_name_translation.csv",
    header=True,
    inferSchema=True
)

print("\n===== KIỂM TRA SỐ DÒNG SAU KHI NẠP DỮ LIỆU =====")
source_tables = {
    "customers": customers,
    "orders": orders,
    "order_items": order_items,
    "payments": payments,
    "reviews": reviews,
    "products": products,
    "sellers": sellers,
    "translation": translation,
}

for table_name, df in source_tables.items():
    print(f"{table_name}: rows={df.count():,}, columns={len(df.columns)}")


# ============================================================
# 3. CHUẨN HÓA KIỂU DỮ LIỆU CƠ BẢN
# ============================================================
orders_typed = (
    orders
    .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_ts", F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_ts", F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_ts", F.to_timestamp("order_estimated_delivery_date"))
)

payments_typed = (
    payments
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_value", F.col("payment_value").cast("double"))
)

order_items_typed = (
    order_items
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
    .withColumn("shipping_limit_ts", F.to_timestamp("shipping_limit_date"))
)

products_typed = (
    products
    .withColumn("product_name_lenght", F.col("product_name_lenght").cast("double"))
    .withColumn("product_description_lenght", F.col("product_description_lenght").cast("double"))
    .withColumn("product_photos_qty", F.col("product_photos_qty").cast("double"))
    .withColumn("product_weight_g", F.col("product_weight_g").cast("double"))
    .withColumn("product_length_cm", F.col("product_length_cm").cast("double"))
    .withColumn("product_height_cm", F.col("product_height_cm").cast("double"))
    .withColumn("product_width_cm", F.col("product_width_cm").cast("double"))
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
    )
)

reviews_typed = (
    reviews
    .withColumn("review_score", F.col("review_score").cast("double"))
    .withColumn("review_creation_ts", F.to_timestamp("review_creation_date"))
    .withColumn("review_answer_ts", F.to_timestamp("review_answer_timestamp"))
)


# ============================================================
# 4. TẠO BẢNG ĐẶC TRƯNG Ở CẤP ĐỘ ĐƠN HÀNG
# ============================================================
# Một order có thể có nhiều dòng payment, nên cần gom về order_id.
payments_agg = (
    payments_typed
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_value_total"),
        F.max("payment_installments").alias("payment_installments_max"),
        F.count("*").alias("payment_count"),
        F.first("payment_type", ignorenulls=True).alias("payment_type")
    )
)

# Một order có thể có nhiều sản phẩm và seller khác nhau.
# Vì vậy cần join order_items với products, sellers, translation rồi gom về order_id.
items_joined = (
    order_items_typed
    .join(products_typed, on="product_id", how="left")
    .join(translation, on="product_category_name", how="left")
    .join(sellers, on="seller_id", how="left")
)

items_agg = (
    items_joined
    .groupBy("order_id")
    .agg(
        F.count("*").alias("item_count"),
        F.countDistinct("product_id").alias("product_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.sum("price").alias("total_price"),
        F.sum("freight_value").alias("total_freight"),
        F.avg("price").alias("avg_price"),
        F.avg("freight_value").alias("avg_freight_value"),
        F.avg("product_name_lenght").alias("avg_product_name_lenght"),
        F.avg("product_description_lenght").alias("avg_product_description_lenght"),
        F.avg("product_photos_qty").alias("avg_product_photos_qty"),
        F.avg("product_weight_g").alias("avg_product_weight_g"),
        F.avg("product_volume_cm3").alias("avg_product_volume_cm3"),
        F.first("product_category_name_english", ignorenulls=True).alias("product_category_name_english"),
        F.first("seller_state", ignorenulls=True).alias("seller_state"),
        F.first("seller_city", ignorenulls=True).alias("seller_city")
    )
)

# Một order có thể có nhiều review_id trong một số trường hợp.
# Lấy điểm review trung bình theo order_id để tạo nhãn ở cấp độ đơn hàng.
reviews_agg = (
    reviews_typed
    .groupBy("order_id")
    .agg(
        F.avg("review_score").alias("review_score"),
        F.count("review_id").alias("review_count")
    )
)

# Join tất cả về bảng ML ở cấp độ order_id.
ml_base = (
    orders_typed
    .join(customers, on="customer_id", how="left")
    .join(reviews_agg, on="order_id", how="inner")
    .join(payments_agg, on="order_id", how="left")
    .join(items_agg, on="order_id", how="left")
)

# Tạo label và các feature thời gian/giao hàng.
ml_df = (
    ml_base
    .withColumn(
        "label",
        F.when(F.col("review_score") >= 4.0, F.lit(1.0)).otherwise(F.lit(0.0))
    )
    .withColumn(
        "delivery_days",
        F.datediff("order_delivered_customer_ts", "order_purchase_ts").cast("double")
    )
    .withColumn(
        "estimated_delivery_days",
        F.datediff("order_estimated_delivery_ts", "order_purchase_ts").cast("double")
    )
    .withColumn(
        "delivery_delay_days",
        F.datediff("order_delivered_customer_ts", "order_estimated_delivery_ts").cast("double")
    )
    .withColumn(
        "approval_hours",
        (
            F.unix_timestamp("order_approved_ts") - F.unix_timestamp("order_purchase_ts")
        ) / F.lit(3600.0)
    )
    .withColumn(
        "is_late",
        F.when(F.col("delivery_delay_days") > 0, F.lit(1.0))
         .when(F.col("delivery_delay_days").isNotNull(), F.lit(0.0))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "freight_ratio",
        F.col("total_freight") / (F.col("total_price") + F.lit(0.000001))
    )
    .withColumn("order_month", F.month("order_purchase_ts").cast("double"))
    .withColumn("order_day_of_week", F.dayofweek("order_purchase_ts").cast("double"))
)

# Các cột feature dùng cho mô hình.
numeric_cols = [
    "payment_value_total",
    "payment_installments_max",
    "payment_count",
    "item_count",
    "product_count",
    "seller_count",
    "total_price",
    "total_freight",
    "avg_price",
    "avg_freight_value",
    "freight_ratio",
    "avg_product_name_lenght",
    "avg_product_description_lenght",
    "avg_product_photos_qty",
    "avg_product_weight_g",
    "avg_product_volume_cm3",
    "delivery_days",
    "estimated_delivery_days",
    "delivery_delay_days",
    "approval_hours",
    "is_late",
    "order_month",
    "order_day_of_week",
]

categorical_cols = [
    "order_status",
    "customer_state",
    "seller_state",
    "payment_type",
    "product_category_name_english",
]

# Cast numeric, xử lý categorical bị thiếu.
for c in numeric_cols:
    ml_df = ml_df.withColumn(c, F.col(c).cast("double"))

for c in categorical_cols:
    ml_df = ml_df.withColumn(
        c,
        F.when((F.col(c).isNull()) | (F.trim(F.col(c).cast("string")) == ""), F.lit("Unknown"))
         .otherwise(F.trim(F.col(c).cast("string")))
    )

ml_df = ml_df.select(
    "order_id",
    "review_score",
    "label",
    *numeric_cols,
    *categorical_cols
).dropna(subset=["label"])

ml_df.cache()

print("\n===== BẢNG DỮ LIỆU ML SAU KHI TẠO FEATURE =====")
print("Số dòng:", ml_df.count())
print("Số cột :", len(ml_df.columns))
ml_df.printSchema()
ml_df.show(5, truncate=False)

print("\n===== PHÂN BỐ NHÃN LABEL =====")
label_distribution = ml_df.groupBy("label").count().orderBy("label")
label_distribution.show()

# Lưu phân bố label ra ảnh.
label_pdf = label_distribution.toPandas()
plt.figure(figsize=(7, 5))
plt.bar(label_pdf["label"].astype(str), label_pdf["count"])
plt.title("Phân bố nhãn review tốt/xấu")
plt.xlabel("Label: 0 = review chưa tốt, 1 = review tốt")
plt.ylabel("Số lượng đơn hàng")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "label_distribution.png", dpi=160)
plt.close()


# ============================================================
# 5. CHIA TRAIN/TEST
# ============================================================
train_data, test_data = ml_df.randomSplit([TRAIN_RATIO, TEST_RATIO], seed=RANDOM_SEED)
train_data.cache()
test_data.cache()

print("\n===== CHIA TẬP TRAIN/TEST =====")
print("Train rows:", train_data.count())
print("Test rows :", test_data.count())


# ============================================================
# 6. PIPELINE TIỀN XỬ LÝ MLLIB
# ============================================================
# Imputer xử lý missing value cho các cột số.
imputed_numeric_cols = [f"{c}_imputed" for c in numeric_cols]

imputer = Imputer(
    inputCols=numeric_cols,
    outputCols=imputed_numeric_cols
).setStrategy("median")

# StringIndexer chuyển categorical variables thành index.
indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=f"{c}_index",
        handleInvalid="keep"
    )
    for c in categorical_cols
]

# OneHotEncoder mã hóa index thành vector one-hot.
encoder = OneHotEncoder(
    inputCols=[f"{c}_index" for c in categorical_cols],
    outputCols=[f"{c}_ohe" for c in categorical_cols],
    dropLast=False
)

# VectorAssembler gộp tất cả đặc trưng thành một vector.
assembler = VectorAssembler(
    inputCols=imputed_numeric_cols + [f"{c}_ohe" for c in categorical_cols],
    outputCol="raw_features",
    handleInvalid="keep"
)

# StandardScaler chuẩn hóa feature.
# withMean=False giúp giữ sparse vector sau OneHotEncoder và tránh tốn bộ nhớ.
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=False
)

preprocess_stages = [imputer] + indexers + [encoder, assembler, scaler]

print("\n===== PIPELINE TIỀN XỬ LÝ =====")
print("Numeric columns:", numeric_cols)
print("Categorical columns:", categorical_cols)
print("Pipeline stages: Imputer -> StringIndexer -> OneHotEncoder -> VectorAssembler -> StandardScaler -> Classifier")


# ============================================================
# 7. KHAI BÁO MÔ HÌNH PHÂN LOẠI
# ============================================================
models = [
    (
        "Logistic Regression",
        LogisticRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=60,
            regParam=0.05,
            elasticNetParam=0.0
        )
    ),
    (
        "Decision Tree Classifier",
        DecisionTreeClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxDepth=8,
            seed=RANDOM_SEED
        )
    ),
    (
        "Random Forest Classifier",
        RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            numTrees=60,
            maxDepth=8,
            seed=RANDOM_SEED
        )
    ),
    (
        "Gradient Boosted Tree Classifier",
        GBTClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=30,
            maxDepth=5,
            seed=RANDOM_SEED
        )
    ),
]

accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)
precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
)
recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedRecall"
)
auc_evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)


# ============================================================
# 8. HUẤN LUYỆN VÀ ĐÁNH GIÁ CÁC MÔ HÌNH
# ============================================================
results = []
fitted_models = {}
predictions_by_model = {}

for model_name, classifier in models:
    print("\n" + "=" * 90)
    print("ĐANG HUẤN LUYỆN MÔ HÌNH:", model_name)
    print("=" * 90)

    pipeline = Pipeline(stages=preprocess_stages + [classifier])
    pipeline_model = pipeline.fit(train_data)
    predictions = pipeline_model.transform(test_data).cache()

    accuracy = accuracy_evaluator.evaluate(predictions)
    f1 = f1_evaluator.evaluate(predictions)
    precision = precision_evaluator.evaluate(predictions)
    recall = recall_evaluator.evaluate(predictions)
    auc = auc_evaluator.evaluate(predictions)

    results.append((model_name, float(accuracy), float(f1), float(precision), float(recall), float(auc)))
    fitted_models[model_name] = pipeline_model
    predictions_by_model[model_name] = predictions

    print(f"Accuracy          : {accuracy:.4f}")
    print(f"F1-score          : {f1:.4f}")
    print(f"Weighted Precision: {precision:.4f}")
    print(f"Weighted Recall   : {recall:.4f}")
    print(f"ROC-AUC           : {auc:.4f}")

    print("\nConfusion matrix:")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

    print("\nMột số dòng dự đoán:")
    cols_to_show = ["order_id", "review_score", "label", "prediction"]
    if "probability" in predictions.columns:
        cols_to_show.append("probability")
    predictions.select(cols_to_show).show(10, truncate=False)

results_df = spark.createDataFrame(
    results,
    ["model", "accuracy", "f1", "weighted_precision", "weighted_recall", "auc"]
)

print("\n===== BẢNG SO SÁNH MÔ HÌNH =====")
results_df.orderBy(F.desc("f1"), F.desc("auc")).show(truncate=False)

# Lưu bảng metric ra CSV local.
results_pdf = results_df.orderBy(F.desc("f1"), F.desc("auc")).toPandas()
results_pdf.to_csv(OUTPUT_DIR / "model_metrics.csv", index=False, encoding="utf-8-sig")

# Vẽ so sánh F1 và ROC-AUC.
plt.figure(figsize=(10, 5))
x_pos = range(len(results_pdf))
plt.bar(x_pos, results_pdf["f1"], label="F1-score")
plt.plot(x_pos, results_pdf["auc"], marker="o", label="ROC-AUC")
plt.xticks(list(x_pos), results_pdf["model"], rotation=30, ha="right")
plt.ylim(0, 1)
plt.title("So sánh F1-score và ROC-AUC giữa các mô hình")
plt.ylabel("Giá trị chỉ số")
plt.legend()
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "model_comparison_f1_auc.png", dpi=160)
plt.close()

# Chọn mô hình tốt nhất theo F1-score, nếu bằng nhau thì xét ROC-AUC.
best_row = results_df.orderBy(F.desc("f1"), F.desc("auc")).first()
best_model_name = best_row["model"]
best_model = fitted_models[best_model_name]
best_predictions = predictions_by_model[best_model_name]

print("\n===== MÔ HÌNH TỐT NHẤT =====")
print("Best model:", best_model_name)
print(f"Accuracy: {best_row['accuracy']:.4f}")
print(f"F1-score: {best_row['f1']:.4f}")
print(f"ROC-AUC : {best_row['auc']:.4f}")


# ============================================================
# 9. CONFUSION MATRIX VÀ TRỰC QUAN HÓA KẾT QUẢ
# ============================================================
confusion_pdf = (
    best_predictions
    .groupBy("label", "prediction")
    .count()
    .orderBy("label", "prediction")
    .toPandas()
)

matrix = pd.DataFrame(
    [[0, 0], [0, 0]],
    index=["Actual 0", "Actual 1"],
    columns=["Predicted 0", "Predicted 1"]
)

for _, row in confusion_pdf.iterrows():
    actual = int(row["label"])
    pred = int(row["prediction"])
    matrix.iloc[actual, pred] = int(row["count"])

matrix.to_csv(OUTPUT_DIR / "confusion_matrix.csv", encoding="utf-8-sig")

plt.figure(figsize=(6, 5))
plt.imshow(matrix.values)
plt.title(f"Confusion Matrix - {best_model_name}")
plt.xticks([0, 1], matrix.columns)
plt.yticks([0, 1], matrix.index)
for i in range(2):
    for j in range(2):
        plt.text(j, i, str(matrix.values[i, j]), ha="center", va="center")
plt.xlabel("Predicted label")
plt.ylabel("Actual label")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "confusion_matrix.png", dpi=160)
plt.close()

# Lưu mẫu dự đoán ra CSV local để đưa vào báo cáo.
select_cols = [
    "order_id",
    "review_score",
    "label",
    "prediction",
    "order_status",
    "customer_state",
    "seller_state",
    "payment_type",
    "product_category_name_english",
    "payment_value_total",
    "total_price",
    "total_freight",
    "delivery_days",
    "delivery_delay_days",
    "is_late",
]

best_predictions.select([c for c in select_cols if c in best_predictions.columns]) \
    .limit(200) \
    .toPandas() \
    .to_csv(OUTPUT_DIR / "sample_predictions.csv", index=False, encoding="utf-8-sig")

# Lưu mô hình tốt nhất ra thư mục Spark ML PipelineModel.
MODEL_OUTPUT_PATH = str(OUTPUT_DIR / "best_pipeline_model")
best_model.write().overwrite().save(MODEL_OUTPUT_PATH)
print("\nĐã lưu best model tại:", MODEL_OUTPUT_PATH)

print("\n===== DANH SÁCH FILE OUTPUT =====")
for path in sorted(OUTPUT_DIR.glob("*")):
    print(path)

print("\nHoàn tất Chương 5 - Spark MLlib.")
