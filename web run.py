import json
import random
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

# Kết nối Kafka Broker
kafka_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/')
def interface():
    return render_template('web_demo.html')

@app.route('/api/send-order', methods=['POST'])
def send_order_to_kafka():
    # Lấy dữ liệu thực tế do khách hàng chọn từ giao diện Front-end gửi lên
    client_data = request.get_json()
    
    raw_price = client_data['price']
    freight = round(random.uniform(10.0, 25.0), 2) # Tự động tính phí vận chuyển giả lập
    
    # Đóng gói bản tin chuẩn chỉnh gửi vào Kafka
    order_payload = {
        "order_id": f"order_rt_{int(time.time() * 1000)}",
        "customer_id": f"cus_rt_{random.randint(10000, 99999)}",
        "order_purchase_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "product_id": client_data['product_id'],
        "product_category_name": client_data['product_category_name'],
        "price": raw_price,
        "freight_value": freight,
        "payment_type": client_data['payment_type'],
        "payment_value": round(raw_price + freight, 2),
        "customer_city": client_data['customer_city']
    }
    
    # Bắn trực tiếp dữ liệu vào orders_topic
    kafka_producer.send('orders_topic', value=order_payload)
    kafka_producer.flush()
    
    return jsonify(order_payload)

if __name__ == '__main__':
    app.run(port=5000)