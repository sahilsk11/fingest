from datetime import datetime
import io
import json
from typing import Callable
import uuid
import boto3
from confluent_kafka import Consumer, Message
import pandas as pd
from domain.broker import Event
from broker import KafkaMessageBroker
from api import get_file_from_s3
import passwords
import sf_import

def get_handler(event_type: str) -> Callable[[dict[str, object]], None]:
    handlers = {
        "FILE_UPLOADED": file_uploaded,
    }
    if event_type not in handlers:
        raise ValueError(f"No handler found for event type {event_type}")
    return handlers[event_type]

s3 = boto3.resource("s3")

wrapper = sf_import.SnowflakeWrapper(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database(),
)
snowflake_import_engine = sf_import.SnowflakeImportEngine(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database(),
    sf_wrapper=wrapper,
)

def file_uploaded(payload: dict[str, object]) -> None:
    # Get the data from the request
    data = payload
    s3_bucket = data.get("s3Bucket")
    s3_file_path = data.get("s3FilePath")
    source_institution = data.get("sourceInstitution")

    file_content, file_type = get_file_from_s3(s3_bucket, s3_file_path)

    if file_type.lower() != "csv":
        raise Exception(f"Unsupported file type: {file_type}")

    csv_to_df = pd.read_csv(io.StringIO(file_content))
    import_run_id = payload.get("importRunId")
    if not import_run_id:
        raise Exception("No import run ID found in payload")
    snowflake_import_engine.import_csv(csv_to_df, source_institution, import_run_id=import_run_id)

def parse_event(msg: Message) -> Event:
    timestamp = None
    if msg.timestamp() and msg.timestamp()[0] == 1:
        # TODO - verify TZ
        timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000)
    return Event(
        event_type=str([x[1] for x in msg.headers() if x[0].lower() == "event"][0], 'utf-8'),
        payload=json.loads(msg.value().decode("utf-8")),
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
                event = parse_event(msg)
                handler = get_handler(event.event_type)
                handler(event.payload)
                # commit
                consumer.commit()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
