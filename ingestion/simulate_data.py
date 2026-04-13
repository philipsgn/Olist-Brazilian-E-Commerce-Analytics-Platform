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
if os.path.exists(DATA_DIR):
    logger.info("[simulate_data] Files found in DATA_DIR: %s", os.listdir(DATA_DIR))
else:
    logger.error("[simulate_data] DATA_DIR does not exist!")

def simulate_new_orders(num_orders=100):
    """
    Script Simulation 2.0: Đảm bảo tính toàn vẹn dữ liệu (Orders -> Items -> Payments)
    """
    # 1. Đọc 3 file CSV chính
    orders_path = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
    items_path = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
    payments_path = os.path.join(DATA_DIR, "olist_order_payments_dataset.csv")

    # [HOTFIX] Tự động tạo file mẫu nếu bị mất
    for path, cols in [
        (orders_path, ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']),
        (items_path, ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value']),
        (payments_path, ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value'])
    ]:
        if not os.path.exists(path):
            logger.warning(f"File missing: {path}. Creating empty template.")
            pd.DataFrame(columns=cols).to_csv(path, index=False)

    df_orders = pd.read_csv(orders_path)
    df_items = pd.read_csv(items_path)
    df_payments = pd.read_csv(payments_path)

    # [HOTFIX] Xử lý trường hợp file trống: Nếu không có dữ liệu để lấy mẫu, tạo dữ liệu trắng hoàn toàn
    if len(df_orders) < num_orders:
        logger.warning("Not enough data to sample. Generating purely synthetic records.")
        # Tạo 1 dòng mẫu giả định để tránh lỗi logic bên dưới
        sample_orders = pd.DataFrame([{
            'order_id': 'placeholder',
            'customer_id': 'template_cust',
            'order_status': 'delivered',
            'order_purchase_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }] * num_orders)
        original_order_ids = ['placeholder'] * num_orders
    else:
        # Chọn ngẫu nhiên [num_orders] đơn hàng mẫu để làm khuôn mẫu mô phỏng
        sample_orders = df_orders[df_orders['order_status'] == 'delivered'].sample(num_orders).copy()
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

    logger.info("PRODUCTION SIMULATION COMPLETE")
    logger.info("Created %s new Orders (Integrity Linked)", num_orders)
    logger.info("Added %s Order Items", len(new_items))
    logger.info("Added %s Payments", len(new_payments))
    logger.info("Timestamps set to: %s", now.strftime("%Y-%m-%d"))

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
