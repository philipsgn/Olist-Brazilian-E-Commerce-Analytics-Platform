"""
FILE: airflow/utils/alerts.py
Purpose: Provide production-grade alerting via Discord Webhooks.
"""

import os
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def on_failure_callback(context):
    """
    Airflow failure callback to send a rich Discord Embed message.
    Triggered when a task fails.
    """
    # Discord Webhook URL from Environment Variable (Security Best Practice)
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    
    if not webhook_url:
        logger.warning("DISCORD_WEBHOOK_URL not set. Skipping alert.")
        return

    # Extract context details
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date').isoformat() if context.get('execution_date') else "N/A"
    error_msg = str(context.get('exception'))[:1000] # Cap error message length
    log_url = context.get('task_instance').log_url
    
    # Environment (Dev/Prod)
    env = os.getenv("ENVIRONMENT", "dev").upper()
    color = 15158332 if env == "PROD" else 3447003 # Red for Prod, Blue for others
    
    # Discord Embed Payload
    payload = {
        "username": "Airflow Bot",
        "avatar_url": "https://airflow.apache.org/images/feature-image.png",
        "embeds": [
            {
                "title": f"🚨 Pipeline Failure | {env}",
                "description": f"A task has failed in the **{dag_id}** pipeline.",
                "color": color,
                "fields": [
                    {"name": "Task ID", "value": f"`{task_id}`", "inline": True},
                    {"name": "Execution Time", "value": f"`{execution_date}`", "inline": True},
                    {"name": "Logs", "value": f"[View Airflow Logs]({log_url})", "inline": False},
                    {"name": "Exception", "value": f"```python\n{error_msg}\n```", "inline": False}
                ],
                "footer": {"text": f"Production Alert System | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Discord alert sent successfully for task {task_id}")
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")
