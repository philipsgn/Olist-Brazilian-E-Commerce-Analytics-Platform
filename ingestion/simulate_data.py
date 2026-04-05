import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

# Cấu hình đường dẫn đến thư mục data hiện tại của em
DATA_DIR = "c:\\Users\\TanPhat\\Documents\\IBM_DATA_ENGINEERING\\ecommerce-de-project\\data"

def simulate_new_orders(num_rows=100):
    """
    Script này giả lập dữ liệu đơn hàng mới bằng cách lấy dữ liệu cũ 
    và tịnh tiến thời gian về hiện tại.
    """
    orders_path = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
    
    if not os.path.exists(orders_path):
        print("Không tìm thấy file orders để giả lập!")
        return

    # Đọc dữ liệu đơn hàng hiện có
    df = pd.read_csv(orders_path)
    
    # Lấy 100 dòng ngẫu nhiên để làm mẫu
    sample_df = df.sample(num_rows).copy()
    
    # Thay đổi ID để không bị trùng (Primary Key)
    sample_df['order_id'] = [f"fake_{os.urandom(8).hex()}" for _ in range(num_rows)]
    
    # Tịnh tiến thời gian: Cho tất cả đơn hàng mới xảy ra vào HÔM NAY
    now = datetime.now()
    sample_df['order_purchase_timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S')
    sample_df['order_approved_at'] = (now + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
    sample_df['order_status'] = 'delivered'
    
    # Append dữ liệu giả vào file CSV thật
    # mode='a' để cộng thêm vào file, không xóa dữ liệu cũ
    sample_df.to_csv(orders_path, mode='a', header=False, index=False)
    print(f"✅ Đã bơm thêm {num_rows} đơn hàng giả vào {orders_path}")

if __name__ == "__main__":
    simulate_new_orders(100)
