# CLI util for posting to kafka

from broker import KafkaMessageBroker

b = KafkaMessageBroker("localhost", 9092)

b.publish("FILE_IMPORT_COMPLETED", {'status': 'added 4 rows to Snowflake table AMEX_CREDIT_CARD_TRANSACTION', 'outputSchema': {'description': '', 'columns': []}, 'importRunId': '70d8fae6-5281-4eac-a779-4b20a0a3ae2e'})