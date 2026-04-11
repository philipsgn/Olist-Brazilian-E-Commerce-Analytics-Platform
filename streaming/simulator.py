"""
streaming/simulator.py
======================
Phase 5 - Streaming Layer
AWS Lambda "Producer" Function

Description:
    Simulates real-time e-commerce order events and sends them
    to Kinesis Data Firehose for delivery to S3 (raw/streaming layer).
    
    This Lambda is triggered on a schedule (e.g., EventBridge every 1 min)
    to continuously generate mock order data for the Lakehouse pipeline.

Architecture:
    EventBridge Scheduler --> Lambda (this) --> Kinesis Firehose --> S3 --> Glue/Athena

Author: TanPhat
Project: Olist Brazilian E-Commerce Analytics Platform
"""

import json
import boto3
import random
import logging
from datetime import datetime

# --- Logger Setup ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- AWS Client ---
firehose = boto3.client('firehose', region_name='ap-southeast-1')

# --- Configuration ---
STREAM_NAME = 'olist-order-stream'  # Kinesis Data Firehose delivery stream name

# --- Static Data Pools ---
ORDER_STATUSES = ['delivered', 'shipped', 'processing', 'canceled']
CUSTOMER_STATES = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'PE', 'CE', 'GO']
PRODUCTS = ['laptop', 'smartphone', 'tablet', 'watch', 'headphones', 'camera', 'keyboard', 'mouse']
PAYMENT_TYPES = ['credit_card', 'voucher', 'debit_card', 'boleto']


def generate_order_record() -> dict:
    """
    Generate a single mock order event.

    Returns:
        dict: A structured order record with randomized fields.
    """
    price = round(random.uniform(50.0, 2000.0), 2)
    freight_value = round(random.uniform(5.0, 80.0), 2)

    return {
        'order_id': f"ORD-{random.randint(100000, 999999)}",
        'customer_state': random.choice(CUSTOMER_STATES),
        'product': random.choice(PRODUCTS),
        'price': price,
        'freight_value': freight_value,
        'total_amount': round(price + freight_value, 2),
        'payment_type': random.choice(PAYMENT_TYPES),
        'order_status': random.choice(ORDER_STATUSES),
        'review_score': random.randint(1, 5),
        'created_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    }


def send_to_firehose(record: dict) -> dict:
    """
    Send a single order record to Kinesis Data Firehose.

    Args:
        record (dict): The order data to send.

    Returns:
        dict: The raw response from Firehose put_record API.

    Raises:
        Exception: Propagates any boto3 client errors.
    """
    payload = json.dumps(record) + '\n'  # Newline delimiter required for S3 JSON Lines format

    response = firehose.put_record(
        DeliveryStreamName=STREAM_NAME,
        Record={'Data': payload.encode('utf-8')}
    )

    logger.info(f"Sent record to Firehose | order_id={record['order_id']} | "
                f"RecordId={response.get('RecordId', 'N/A')}")
    return response


def lambda_handler(event, context):
    """
    AWS Lambda entry point.

    Generates one mock order record per invocation and publishes it to Firehose.
    Designed to be triggered by EventBridge Scheduler on a fixed interval.

    Args:
        event (dict): Lambda event payload (unused for scheduled triggers).
        context (LambdaContext): Lambda runtime context.

    Returns:
        dict: HTTP-style response with statusCode and body.
    """
    try:
        record = generate_order_record()
        response = send_to_firehose(record)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully sent record to Kinesis Firehose',
                'order_id': record['order_id'],
                'firehose_stream': STREAM_NAME,
                'RecordId': response.get('RecordId')
            })
        }

    except Exception as e:
        logger.error(f"Failed to send record to Firehose: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal error',
                'error': str(e)
            })
        }
