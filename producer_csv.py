import csv
import json
import time
import os
from kafka import KafkaProducer

CSV_FILE = "2025-C.csv"          
TOPIC_NAME = "water-quality"     
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

SLEEP_SECONDS = 1


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
    )
    return producer


def stream_csv_to_kafka():
    producer = create_producer()

    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for idx, row in enumerate(reader, start=1):
            # sử dụng @id làm key (nếu có), nếu không thì dùng index
            key = row.get("@id") or str(idx)

            # chuẩn bị message
            message_value = row

            producer.send(
                TOPIC_NAME,
                key=key,
                value=message_value
            )

            print(f"[SENT] #{idx} key={key} value={message_value}")

            # ngủ 1 chút để giả lập streaming real-time
            time.sleep(SLEEP_SECONDS)

    producer.flush()
    print("✅ DONE: Đã gửi hết dữ liệu từ CSV lên Kafka.")


if __name__ == "__main__":
    stream_csv_to_kafka()
