import json
from typing import Optional
import uuid
from confluent_kafka import Producer


class MessageBroker:
    def __init__(self):
        return self

    def publish(self, topic: str, payload: dict[str, object]) -> None:
        raise NotImplementedError("Subclasses must implement this method.")


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
        import_run_id: Optional[uuid.UUID] = None,
    ) -> None:
        if import_run_id:
            payload["importRunId"] = str(import_run_id)
        self.producer.produce(
            self.topic, json.dumps(payload), headers={"event": event_name}
        )
        self.producer.flush()
        print(f"Message sent to {self.topic} with payload {payload}")
