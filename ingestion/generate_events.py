import json
import os
import random
import time
from datetime import datetime, timezone

import pika
import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

DB_URL = os.environ["DB_URL"]
AMQP_URL = os.environ["AMQP_URL"]
QUEUE_NAME = "orders_events"
fake = Faker()


def get_amqp_channel():
    params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    return connection, channel


def publish_event(channel, event_type, table, record):
    message = {
        "event_type": event_type,
        "table": table,
        "record": record,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),
    )

PRODUCT_NAMES = [
    "Wireless Mouse", "Mechanical Keyboard", "USB-C Hub", "Laptop Stand",
    "Noise Cancelling Headphones", "Webcam", "Monitor Arm", "Desk Lamp",
    "Portable SSD", "Bluetooth Speaker",
]


def get_connection():
    return psycopg2.connect(DB_URL)


def ensure_inventory_seeded(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM inventory;")
        (count,) = cur.fetchone()
        if count == 0:
            for name in PRODUCT_NAMES:
                cur.execute(
                    "INSERT INTO inventory (product_name, stock_quantity) VALUES (%s, %s);",
                    (name, random.randint(50, 200)),
                )
            conn.commit()


def create_customer(conn, channel):
    name, email = fake.name(), fake.unique.email()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING customer_id;",
            (name, email),
        )
        (customer_id,) = cur.fetchone()
        conn.commit()
    publish_event(
        channel, "INSERT", "customers",
        {"customer_id": customer_id, "name": name, "email": email},
    )
    return customer_id


def place_order(conn, channel, customer_id):
    amount = round(random.uniform(10, 500), 2)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer_id, status, total_amount) "
            "VALUES (%s, 'placed', %s) RETURNING order_id;",
            (customer_id, amount),
        )
        (order_id,) = cur.fetchone()
        conn.commit()
    publish_event(
        channel, "INSERT", "orders",
        {"order_id": order_id, "customer_id": customer_id, "status": "placed", "total_amount": amount},
    )
    return order_id, amount


def create_payment(conn, channel, order_id, amount):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO payments (order_id, status, amount) "
            "VALUES (%s, 'pending', %s) RETURNING payment_id;",
            (order_id, amount),
        )
        (payment_id,) = cur.fetchone()
        conn.commit()
    publish_event(
        channel, "INSERT", "payments",
        {"payment_id": payment_id, "order_id": order_id, "status": "pending", "amount": amount},
    )
    return payment_id


def advance_payment(conn, channel, payment_id, order_id):
    outcome = random.choices(
        ["completed", "failed"], weights=[0.9, 0.1], k=1
    )[0]
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE payments SET status = %s, updated_at = now() WHERE payment_id = %s;",
            (outcome, payment_id),
        )
        order_status = "paid" if outcome == "completed" else "payment_failed"
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s;",
            (order_status, order_id),
        )
        conn.commit()
    publish_event(
        channel, "UPDATE", "payments",
        {"payment_id": payment_id, "order_id": order_id, "status": outcome},
    )
    publish_event(
        channel, "UPDATE", "orders",
        {"order_id": order_id, "status": order_status},
    )
    return outcome


def decrement_inventory(conn, channel):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT product_id, stock_quantity FROM inventory "
            "WHERE stock_quantity > 0 ORDER BY random() LIMIT 1;"
        )
        row = cur.fetchone()
        if row is None:
            return
        product_id, stock = row
        cur.execute(
            "UPDATE inventory SET stock_quantity = stock_quantity - 1, updated_at = now() "
            "WHERE product_id = %s;",
            (product_id,),
        )
        conn.commit()
    publish_event(
        channel, "UPDATE", "inventory",
        {"product_id": product_id, "stock_quantity": stock - 1},
    )


def maybe_cancel_order(conn, channel, order_id):
    if random.random() < 0.05:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = 'cancelled', updated_at = now() WHERE order_id = %s;",
                (order_id,),
            )
            conn.commit()
        publish_event(
            channel, "UPDATE", "orders",
            {"order_id": order_id, "status": "cancelled"},
        )
        return True
    return False


def run_once(conn, channel):
    customer_id = create_customer(conn, channel)
    order_id, amount = place_order(conn, channel, customer_id)
    payment_id = create_payment(conn, channel, order_id, amount)
    decrement_inventory(conn, channel)
    outcome = advance_payment(conn, channel, payment_id, order_id)
    cancelled = False
    if outcome == "completed":
        cancelled = maybe_cancel_order(conn, channel, order_id)

    print(
        f"customer={customer_id} order={order_id} amount={amount} "
        f"payment={outcome} cancelled={cancelled}"
    )


def main(iterations=20, delay_seconds=1.0):
    conn = get_connection()
    amqp_connection, channel = get_amqp_channel()
    try:
        ensure_inventory_seeded(conn)
        for _ in range(iterations):
            run_once(conn, channel)
            time.sleep(delay_seconds)
    finally:
        conn.close()
        amqp_connection.close()


if __name__ == "__main__":
    main()
