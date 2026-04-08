import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging

# Module-level logger
logger = logging.getLogger(__name__)

# Tự động nhận diện đường dẫn (Local hoặc Docker)
# Override: set DATA_DIR env var before running (e.g. on EC2 or inside Airflow).
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
logger.info("[simulate_data] DATA_DIR resolved to: %s", DATA_DIR)

def simulate_new_orders(num_orders=100):
    """
    Script Simulation 2.0: Đảm bảo tính toàn vẹn dữ liệu (Orders -> Items -> Payments)
    """
    # 1. Đọc 3 file CSV chính
    orders_path = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
    items_path = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
    payments_path = os.path.join(DATA_DIR, "olist_order_payments_dataset.csv")

    if not all(os.path.exists(p) for p in [orders_path, items_path, payments_path]):
        print("🚨 Lỗi: Thiếu 1 trong 3 file CSV cần thiết!")
        return

    df_orders = pd.read_csv(orders_path)
    df_items = pd.read_csv(items_path)
    df_payments = pd.read_csv(payments_path)

    # 2. Chọn ngẫu nhiên [num_orders] đơn hàng mẫu để làm khuôn mẫu mô phỏng
    # Chúng ta lấy các đơn hàng 'delivered' để mô phỏng cho thật
    sample_orders = df_orders[df_orders['order_status'] == 'delivered'].sample(num_orders).copy()
    
    # Danh sách các order_id gốc để tìm items và payments tương ứng
    original_order_ids = sample_orders['order_id'].tolist()
    
    # 3. Tạo mapping ID mới (Fake ID) để duy trì mối quan hệ (Relational Mapping)
    new_id_map = {old: f"fake_{os.urandom(8).hex()}" for old in original_order_ids}

    # --- XỬ LÝ BẢNG ORDERS ---
    sample_orders['order_id'] = sample_orders['order_id'].map(new_id_map)
    now = datetime.now()
    # Giả lập thời gian mua hàng là HÔM NAY
    sample_orders['order_purchase_timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S')
    sample_orders['order_approved_at'] = (now + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
    sample_orders['order_delivered_carrier_date'] = (now + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    sample_orders['order_delivered_customer_date'] = (now + timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    sample_orders['order_estimated_delivery_date'] = (now + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

    # --- XỬ LÝ BẢNG ITEMS ---
    # Tìm tất cả items thuộc về các đơn hàng gốc đã chọn
    new_items = df_items[df_items['order_id'].isin(original_order_ids)].copy()
    new_items['order_id'] = new_items['order_id'].map(new_id_map)

    # --- XỬ LÝ BẢNG PAYMENTS ---
    # Tìm tất cả payments thuộc về các đơn hàng gốc đã chọn
    new_payments = df_payments[df_payments['order_id'].isin(original_order_ids)].copy()
    new_payments['order_id'] = new_payments['order_id'].map(new_id_map)

    # 4. Ghi nối (Append) vào các file CSV gốc
    sample_orders.to_csv(orders_path, mode='a', header=False, index=False)
    new_items.to_csv(items_path, mode='a', header=False, index=False)
    new_payments.to_csv(payments_path, mode='a', header=False, index=False)

    print(f"✅ PRODUCTION SIMULATION COMPLETE:")
    print(f"   -> Created {num_orders} new Orders (Integrity Linked)")
    print(f"   -> Added {len(new_items)} Order Items")
    print(f"   -> Added {len(new_payments)} Payments")
    print(f"   -> Timestamps set to: {now.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    # When running standalone (e.g. python simulate_data.py on local or EC2),
    # auto-load the .env file in the project root so POSTGRES_* vars are available
    # without manually exporting them in the shell.
    try:
        from dotenv import load_dotenv
        load_dotenv()  # looks for .env starting from cwd upward
        logger.info("[simulate_data] .env loaded via python-dotenv")
    except ImportError:
        pass  # On production containers dotenv is not needed; env vars are injected

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    simulate_new_orders(100)
