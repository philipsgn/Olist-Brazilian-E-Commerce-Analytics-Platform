import random
import time
import json
from datetime import datetime

# Danh sách mẫu để sinh dữ liệu ngẫu nhiên
PRODUCTS = ["smartphone", "laptop", "headphones", "book", "shirt", "shoes"]
STATES   = ["SP", "RJ", "MG", "RS", "PR"]  # Các tiểu bang Brazil
PAYMENT  = ["credit_card", "boleto", "voucher", "debit_card"]

def generate_fake_order():
    """Tạo 1 đơn hàng giả lập"""
    return {
        "order_id":        f"ORD-{random.randint(100000, 999999)}",
        "customer_state":  random.choice(STATES),
        "product":         random.choice(PRODUCTS),
        "price":           round(random.uniform(20, 500), 2),
        "payment_type":    random.choice(PAYMENT),
        "created_at":      datetime.now().isoformat(),
    }

def stream_orders(interval_seconds=3):
    """Sinh đơn hàng liên tục, cách nhau interval_seconds giây"""
    print("Bắt đầu stream đơn hàng... (Ctrl+C để dừng)")
    count = 0
    while True:
        order = generate_fake_order()
        count += 1
        print(f"[#{count}] Đơn mới: {order['order_id']} | "
              f"{order['product']} | ${order['price']}")
        # Sau này sẽ lưu vào database thay vì chỉ print
        time.sleep(interval_seconds)

if __name__ == "__main__":
    stream_orders()