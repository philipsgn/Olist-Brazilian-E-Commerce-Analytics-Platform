import requests
import json
import os
import logging

# Cấu hình logging để dễ dàng debug trong logs của Airflow Worker
logger = logging.getLogger(__name__)

def send_discord_alert(context):
    """
    Hàm callback gửi thông báo lỗi từ Airflow lên Discord.
    Đã được tối ưu (try-except) để tuyệt đối không làm crash Task/Worker nếu Webhook lỗi.
    """
    # 1. Lấy Webhook URL từ biến môi trường (An toàn hơn hardcode)
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    
    if not webhook_url:
        logger.warning("⚠️ DISCORD_WEBHOOK_URL is not set in environment variables. Skipping alert.")
        return

    try:
        # 2. Thu thập thông tin từ context của Airflow
        ti = context.get('task_instance')
        dag_id = ti.dag_id
        task_id = ti.task_id
        
        # Lấy thời gian chạy (hỗ trợ cả logical_date của Airflow 2.2+ và execution_date cũ)
        execution_date = context.get('logical_date') or context.get('execution_date')
        exec_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S') if execution_date else "N/A"
        
        log_url = ti.log_url
        exception = context.get('exception')

        # 3. Format message an toàn (Discord giới hạn payload size)
        error_message = str(exception)[:500] + "..." if len(str(exception)) > 500 else str(exception)

        # 4. Xác định môi trường để hiển thị màu sắc
        env = os.getenv("ENVIRONMENT", "DEV").upper()
        # Màu đỏ (15158332) cho lỗi, hoặc màu cam (15105570)
        color = 15158332 if env == "PROD" else 15105570

        payload = {
            "username": f"Airflow Alert [{env}]",
            "avatar_url": "https://airflow.apache.org/images/feature-image.png",
            "embeds": [{
                "title": "❌ Pipeline Task Failed",
                "description": f"Phát ơi! Đã xảy ra lỗi tại DAG **{dag_id}**",
                "color": color,
                "fields": [
                    {"name": "DAG ID", "value": f"`{dag_id}`", "inline": True},
                    {"name": "Task ID", "value": f"`{task_id}`", "inline": True},
                    {"name": "Execution Time", "value": f"`{exec_date_str}`", "inline": False},
                    {"name": "Error Details", "value": f"```python\n{error_message}\n```", "inline": False},
                    {"name": "Check Logs", "value": f"[Bấm vào đây để xem Log chi tiết]({log_url})", "inline": False}
                ],
                "footer": {
                    "text": "E-commerce Pipeline Monitoring | AWS EC2 Deployment"
                },
                "timestamp": execution_date.isoformat() if execution_date else None
            }]
        }

        # 5. Gửi request tới Discord Webhook
        response = requests.post(
            webhook_url, 
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        # Kiểm tra phản hồi nhưng không raise Exception để tránh crash task chính
        if response.status_code == 204 or response.status_code == 200:
            logger.info(f"✅ Successfully sent failure alert for {task_id} to Discord.")
        else:
            logger.error(f"❌ Discord Webhook returned status {response.status_code}: {response.text}")

    except Exception as e:
        # Bọc toàn bộ trong try-except để đảm bảo logic thông báo không làm ảnh hưởng đến Pipeline
        logger.error(f"❌ Critical error in send_discord_alert: {str(e)}")
