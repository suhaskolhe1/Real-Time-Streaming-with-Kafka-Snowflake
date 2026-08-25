#!/usr/bin/env python3
"""
Send a single sensor message to a Kafka topic.
Usage: python send_message.py '<JSON_MESSAGE>'

Example:
python send_message.py '{"device_id": "DEVICE_001", "sensor_type": "temperature", "value": 23.5, "unit": "celsius", "timestamp": "2024-01-15T10:30:00Z", "location": {"building": "HQ", "floor": 2, "zone": "A"}}'
"""

import sys
import json
import argparse
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
TOPIC_NAME = 'sensor_data'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def send_message(message_json):
    producer = create_producer()
    
    try:
        message = json.loads(message_json)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}")
        sys.exit(1)
    
    required_fields = ['device_id', 'sensor_type', 'value', 'unit', 'timestamp', 'location']
    missing_fields = [f for f in required_fields if f not in message]
    if missing_fields:
        print(f"Warning: Missing fields: {', '.join(missing_fields)}")
    
    device_id = message.get('device_id', 'UNKNOWN')
    
    try:
        future = producer.send(
            TOPIC_NAME, 
            key=device_id,
            value=message
        )
        record_metadata = future.get(timeout=10)
        print(f"Message sent successfully!")
        print(f"  Topic: {record_metadata.topic}")
        print(f"  Partition: {record_metadata.partition}")
        print(f"  Offset: {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")
        sys.exit(1)
    finally:
        producer.close()

def main():
    parser = argparse.ArgumentParser(
        description='Send a single sensor message to the sensor_data Kafka topic'
    )
    parser.add_argument(
        'message',
        type=str,
        help='JSON message to send (must be a valid JSON object)'
    )
    
    args = parser.parse_args()
    
    if not args.message:
        print("Error: Message cannot be empty")
        sys.exit(1)
    
    send_message(args.message)

if __name__ == "__main__":
    main()