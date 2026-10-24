import io
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
import pika
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

AMQP_URL = os.environ["AMQP_URL"]
QUEUE_NAME = "orders_events"
BUCKET = "realtime-lakehouse-bronze"
PREFIX = "bronze/orders_events"
BATCH_SIZE = 100

endpoint = os.environ["B2_S3_ENDPOINT"]
if "://" not in endpoint:
    endpoint = f"https://{endpoint}"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    region_name=endpoint.split(".")[1],
    config=Config(
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    ),
)


def to_row(body):
    event = json.loads(body)
    return {
        "event_type": event["event_type"],
        "source_table": event["table"],
        "emitted_at": event["emitted_at"],
        "payload": json.dumps(event["record"]),
    }


def write_batch(rows):
    now = datetime.now(timezone.utc)
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    key = f"{PREFIX}/dt={now:%Y-%m-%d}/hour={now:%H}/{uuid.uuid4().hex}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    return key


def flush(channel, rows, last_tag):
    key = write_batch(rows)
    channel.basic_ack(delivery_tag=last_tag, multiple=True)
    print(f"wrote {len(rows)} events to {key}")
    return len(rows)


def drain_queue():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    rows, last_tag, written = [], None, 0
    try:
        while True:
            method, _, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
            if method is None:
                break
            rows.append(to_row(body))
            last_tag = method.delivery_tag
            if len(rows) == BATCH_SIZE:
                written += flush(channel, rows, last_tag)
                rows = []
        if rows:
            written += flush(channel, rows, last_tag)
    finally:
        connection.close()
    print(f"done, {written} events landed in B2")


if __name__ == "__main__":
    drain_queue()
