"""
FILE: airflow/utils/discord_alerts.py
Author: Senior Data Engineer
Purpose: Gửi thông báo lỗi pipeline tới Discord Webhook (Chuẩn Production).
"""

import os
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def send_discord_alert(context):
    """
    Callback function khi Task thất bại.
    Airflow sẽ tự động truyền 'context' chứa thông tin lỗi vào đây.
    """
    # 1. Lấy Webhook URL từ biến môi trường (Bảo mật)
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    
    if not webhook_url:
        logger.warning("⚠️ DISCORD_WEBHOOK_URL chưa được cấu hình. Bỏ qua gửi thông báo.")
        return

    # 2. Trích xuất thông tin từ context của Airflow
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    
    # Xử lý thời gian thực thi (hỗ trợ cả Pendulum datetime)
    exec_date_raw = context.get('execution_date')
    exec_date = exec_date_raw.strftime('%Y-%m-%d %H:%M:%S') if exec_date_raw else "N/A"
    
    log_url = context.get('task_instance').log_url
    exception = str(context.get('exception'))[:500]  # Lấy 500 ký tự đầu của lỗi
    
    # Xác định môi trường (Dev/Prod) để đổi màu thông báo
    env = os.getenv("ENVIRONMENT", "dev").upper()
    hex_color = 15158332 if env == "PROD" else 3447003 # Đỏ cho Prod, Xanh cho Dev

    # 3. Cấu trúc nội dung gửi tới Discord (Dạng Embed chuyên nghiệp)
    payload = {
        "username": f"Airflow Bot ({env})",
        "avatar_url": "https://airflow.apache.org/images/feature-image.png",
        "embeds": [{
            "title": "🚨 CẢNH BÁO PIPELINE THẤT BẠI",
            "description": f"Phát ơi! Pipeline **{dag_id}** vừa gặp sự cố.",
            "color": hex_color,
            "fields": [
                {"name": "Môi trường", "value": f"`{env}`", "inline": True},
                {"name": "Task ID", "value": f"`{task_id}`", "inline": True},
                {"name": "Thời gian chạy", "value": f"`{exec_date}`", "inline": False},
                {"name": "Lỗi chi tiết", "value": f"```python\n{exception}\n```", "inline": False},
                {"name": "Kiểm tra Logs ngay tại đây", "value": f"[Bấm vào để xem Log]({log_url})", "inline": False}
            ],
            "footer": {"text": "Hệ thống giám sát dữ liệu E-commerce | AWS EC2 Deployment"}
        }]
    }

    # 4. Gửi request tới Discord kèm cơ chế xử lý lỗi
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"✅ Đã gửi thông báo lỗi Task {task_id} tới Discord.")
    except Exception as e:
        logger.error(f"❌ Không thể gửi thông báo tới Discord: {e}")
