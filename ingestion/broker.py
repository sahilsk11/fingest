import json
from typing import Optional
import uuid
from confluent_kafka import Producer

IMPORT_RUN_STATE_UPDATED = "IMPORT_RUN_STATE_UPDATED"

class MessageBroker:
    def publish(self, topic: str, payload: dict[str, object]) -> None:
        raise NotImplementedError("Subclasses must implement this method.")

class DummyMessageBroker(MessageBroker):
    def publish(self, topic: str, payload: dict[str, object]) -> None:
        print(f"DummyMessageBroker: publishing to {topic} with payload {payload}")

class KafkaMessageBroker(MessageBroker):
    def __init__(self, host="localhost", port=9092):
        self.producer = Producer(
            {
                "bootstrap.servers": f"{host}:{port}",
            }
        )
        self.topic = "DEFAULT"

    def publish(
        self,
        event_name: str,
        payload: dict[str, object],
        # shorthand for including in payload
        import_run_id: Optional[uuid.UUID] = None,
    ) -> None:
        if import_run_id:
            payload["importRunId"] = str(import_run_id)
        self.producer.produce(
            self.topic, json.dumps(payload), headers={"event": event_name}
        )
        self.producer.flush()
        print(f"publishing event {event_name} with payload {payload}")
