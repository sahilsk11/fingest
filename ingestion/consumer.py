from datetime import datetime
import io
import json
import logging
from typing import Callable, Optional, Tuple
import uuid
import boto3
from confluent_kafka import Consumer, Message
import pandas as pd
from domain.broker import Event
from broker import KafkaMessageBroker, MessageBroker
from domain.normalizer import TransformerOutputSchema
from normalizer import NormalizationService
import passwords
import sf_import


class ConsumerResolver:
    def __init__(
        self,
        host: str,
        port: int,
        sf_wrapper: sf_import.SnowflakeWrapper,
        snowflake_import_engine: sf_import.SnowflakeImportEngine,
        broker: MessageBroker,
        normalization_service: NormalizationService,
    ):
        self.wrapper = sf_wrapper
        self.snowflake_import_engine = snowflake_import_engine
        self.broker = broker
        self.normalization_service = normalization_service

        group_id = "default-group-id"  # str(uuid.uuid4())
        self.consumer = Consumer(
            {
                "bootstrap.servers": f"{host}:{port}",
                "group.id": group_id,
                # "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe(["DEFAULT"])
        self.s3 = boto3.resource("s3")

    def _get_file_from_s3(self, bucket_name: str, file_path: str) -> Tuple[str, str]:
        obj = self.s3.Object(bucket_name, file_path)
        file_type = file_path.split(".")[-1]
        # NotAuthorized here probably means file not found
        content = obj.get()["Body"].read().decode("utf-8")
        return content, file_type

    def file_import_completed_resolver(self, payload: dict[str, object]) -> None:
        import_run_id = payload.get("importRunId")
        if not import_run_id:
            raise Exception("No import run ID found in payload")
        output_schema = payload.get("outputSchema")
        if not output_schema:
            raise Exception("No output schema found in payload")

        # Convert to expected types
        import_run_id = uuid.UUID(str(import_run_id))
        output_schema = TransformerOutputSchema(**output_schema)  # type: ignore

        self.normalization_service.normalize_data(import_run_id, output_schema)

    def file_uploaded_handler_resolver(self, payload: dict[str, object]) -> None:
        # Get the data from the request
        data = payload
        s3_bucket = str(data.get("s3Bucket", ""))
        s3_file_path = str(data.get("s3FilePath", ""))
        source_institution = str(data.get("sourceInstitution", ""))
        output_schema = payload.get("outputSchema")
        if not output_schema:
            raise Exception("No output schema found in payload")

        if not s3_bucket or not s3_file_path:
            raise Exception("No s3 bucket or file path found in payload")

        file_content, file_type = self._get_file_from_s3(s3_bucket, s3_file_path)

        if file_type.lower() != "csv":
            raise Exception(f"Unsupported file type: {file_type}")

        output_schema = TransformerOutputSchema(**output_schema)  # type: ignore

        csv_to_df = pd.read_csv(io.StringIO(file_content))
        import_run_id_str = str(payload.get("importRunId", ""))
        if not import_run_id_str:
            raise Exception("No import run ID found in payload")
        import_run_id = uuid.UUID(import_run_id_str)

        if not import_run_id:
            raise Exception("No import run ID found in payload")
        self.snowflake_import_engine.import_csv(
            csv_to_df,
            source_institution,
            import_run_id=import_run_id,
            output_schema=output_schema,
        )

    def get_handler(
        self, event_type: str
    ) -> Optional[Callable[[dict[str, object]], None]]:
        handlers = {
            "FILE_UPLOADED": self.file_uploaded_handler_resolver,
            "FILE_IMPORT_COMPLETED": self.file_import_completed_resolver,
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

    event_header = next(
        (x[1] for x in msg.headers() or [] if x[0].lower() == "event"), None
    )
    if not event_header:
        raise ValueError("No event header found in message")

    value = msg.value()
    if not value:
        payload = {}
    else:
        payload = json.loads(
            value.decode("utf-8") if isinstance(value, bytes) else str(value)
        )

    return Event(
        event_type=(
            event_header.decode("utf-8")
            if isinstance(event_header, bytes)
            else str(event_header)
        ),
        payload=payload,
        timestamp=timestamp,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    host = "localhost"
    port = 9092

    sf_wrapper = sf_import.SnowflakeWrapper(
        user=passwords.get_snowflake_user(),
        password=passwords.get_snowflake_password(),
        account=passwords.get_snowflake_account(),
        schema=passwords.get_snowflake_schema(),
        database=passwords.get_snowflake_database(),
    )
    broker = KafkaMessageBroker(host, port)
    snowflake_import_engine = sf_import.SnowflakeImportEngine(
        user=passwords.get_snowflake_user(),
        password=passwords.get_snowflake_password(),
        account=passwords.get_snowflake_account(),
        schema=passwords.get_snowflake_schema(),
        database=passwords.get_snowflake_database(),
        sf_wrapper=sf_wrapper,
        broker=broker,
    )
    normalization_service = NormalizationService(
        sf_wrapper,
        broker,
    )
    c = ConsumerResolver(
        host,
        port,
        sf_wrapper,
        snowflake_import_engine,
        KafkaMessageBroker(host, port),
        normalization_service=normalization_service,
    )
    c.run()
