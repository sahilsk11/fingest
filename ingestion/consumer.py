from datetime import datetime
import json
import uuid
from confluent_kafka import Consumer, Message
from domain.broker import Event
from broker import KafkaMessageBroker



def parse_event(msg: Message) -> Event:
    payload = json.loads(msg.value().decode("utf-8"))
    timestamp = None
    if msg.timestamp() and msg.timestamp()[0] == 1:
        # TODO - verify TZ
        timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000)
    return Event(
        event_type=str([x[1] for x in msg.headers() if x[0].lower() == "event"][0], 'utf-8'),
        payload=json.loads(msg.value().decode("utf-8")),
        import_run_id=payload.get("importRunId"),
        timestamp=timestamp,
    )

if __name__ == "__main__":
    host = "localhost"
    port = 9092
    group_id = "default-group-id" #str(uuid.uuid4())
    consumer = Consumer(
        {
            "bootstrap.servers": f"{host}:{port}",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["DEFAULT"])
    broker = KafkaMessageBroker(host, port).publish(
        "FILE_UPLOADED",
        {
            "s3Bucket": "bucket",
            "s3FilePath": "path/to/file.csv",
            "sourceInstitution": "sourceInstitution",
        },
        import_run_id=uuid.uuid4(),
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            elif msg is not None:
                print(parse_event(msg))
                # commit
                consumer.commit()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
