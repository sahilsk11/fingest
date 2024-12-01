from datetime import datetime
import io
import json
from typing import Callable, Optional, Tuple
import uuid
import boto3
from confluent_kafka import Consumer, Message
import pandas as pd
from domain.broker import Event
from broker import KafkaMessageBroker
from normalizer import normalize_data
import passwords
import sf_import


s3 = boto3.resource("s3")

class ConsumerResolver:
    def __init__(self, host, port, sf_wrapper, snowflake_import_engine, broker):
        self.wrapper = sf_wrapper
        self.snowflake_import_engine = snowflake_import_engine
        self.broker = broker

        group_id = "default-group-id" #str(uuid.uuid4())
        self.consumer = Consumer(
            {
                "bootstrap.servers": f"{host}:{port}",
                "group.id": group_id,
                # "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe(["DEFAULT"])

    def _get_file_from_s3(self, bucket_name: str, file_path: str) -> Tuple[str, str]:
        obj = s3.Object(self, bucket_name, file_path)
        file_type = file_path.split(".")[-1]
        # NotAuthorized here probably means file not found
        content = obj.get()["Body"].read().decode("utf-8")
        return content, file_type

    def file_import_completed(self, payload: dict[str, object]) -> None:
        import_run_id = payload.get("importRunId")
        if not import_run_id:
            raise Exception("No import run ID found in payload")
        normalize_data(self.wrapper, import_run_id, self.broker)

    def file_uploaded(self, payload: dict[str, object]) -> None:
        # Get the data from the request
        data = payload
        s3_bucket = data.get("s3Bucket")
        s3_file_path = data.get("s3FilePath")
        source_institution = data.get("sourceInstitution")

        file_content, file_type = self._get_file_from_s3(s3_bucket, s3_file_path)

        if file_type.lower() != "csv":
            raise Exception(f"Unsupported file type: {file_type}")

        csv_to_df = pd.read_csv(io.StringIO(file_content))
        import_run_id = payload.get("importRunId")
        if not import_run_id:
            raise Exception("No import run ID found in payload")
        self.snowflake_import_engine.import_csv(csv_to_df, source_institution, import_run_id=import_run_id)

    def get_handler(self, event_type: str) -> Optional[Callable[[dict[str, object]], None]]:
        handlers = {
            "FILE_UPLOADED": self.file_uploaded,
            "FILE_IMPORT_COMPLETED": self.file_import_completed,
        }
        if event_type not in handlers:
            return None
        return handlers[event_type]
    
    def run(self):
        print("consumer started")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    # print("Waiting...")
                    pass
                elif msg.error():
                    print(f"ERROR: {msg.error()}")
                elif msg is not None:
                    # print(msg)
                    event = parse_event(msg)
                    handler = self.get_handler(event.event_type)
                    if handler is not None:
                        handler(event.payload)
                    # commit
                    self.consumer.commit()
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            print("consumer closed")



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

    sf_wrapper = sf_import.SnowflakeWrapper(
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
        sf_wrapper=sf_wrapper,
        broker=KafkaMessageBroker(host, port),
    )
    c = ConsumerResolver(host, port, sf_wrapper, snowflake_import_engine, KafkaMessageBroker(host, port))
    c.run()
    
