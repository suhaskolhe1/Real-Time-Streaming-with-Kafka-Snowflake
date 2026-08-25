#!/usr/bin/env python3
"""
Generate random IoT sensor data and send to Kafka.
Usage: python generate_sensor_data.py --duration 1 --rate 10
"""

import argparse
import json
import random
import time
import signal
import sys
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
TOPIC_NAME = 'sensor_data'

DEVICE_IDS = [f"DEVICE_{i:03d}" for i in range(1, 21)]
SENSOR_TYPES = {
    "temperature": {"min": 15.0, "max": 35.0, "unit": "celsius"},
    "humidity": {"min": 20.0, "max": 80.0, "unit": "percent"},
    "pressure": {"min": 980.0, "max": 1040.0, "unit": "hPa"},
    "co2": {"min": 400.0, "max": 2000.0, "unit": "ppm"},
    "light": {"min": 0.0, "max": 10000.0, "unit": "lux"}
}
BUILDINGS = ["HQ", "WAREHOUSE_A", "WAREHOUSE_B", "FACTORY_1", "FACTORY_2"]
ZONES = ["A", "B", "C", "D"]

running = True

def signal_handler(sig, frame):
    global running
    print("\nShutting down gracefully...")
    running = False

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        batch_size=16384,
        linger_ms=10,
        acks='all'
    )

def generate_sensor_reading():
    device_id = random.choice(DEVICE_IDS)
    sensor_type = random.choice(list(SENSOR_TYPES.keys()))
    sensor_config = SENSOR_TYPES[sensor_type]
    
    value = round(
        random.uniform(sensor_config["min"], sensor_config["max"]), 
        2
    )
    
    return {
        "device_id": device_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": sensor_config["unit"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": {
            "building": random.choice(BUILDINGS),
            "floor": random.randint(1, 5),
            "zone": random.choice(ZONES)
        }
    }

def run_generator(duration_minutes, messages_per_second):
    global running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    producer = create_producer()
    
    interval = 1.0 / messages_per_second
    end_time = time.time() + (duration_minutes * 60)
    
    messages_sent = 0
    start_time = time.time()
    
    print(f"Starting data generation:")
    print(f"  Duration: {duration_minutes} minute(s)")
    print(f"  Rate: {messages_per_second} messages/second")
    print(f"  Topic: {TOPIC_NAME}")
    print(f"Press Ctrl+C to stop early\n")
    
    try:
        while running and time.time() < end_time:
            batch_start = time.time()
            
            for _ in range(messages_per_second):
                if not running:
                    break
                    
                message = generate_sensor_reading()
                producer.send(
                    TOPIC_NAME,
                    key=message["device_id"],
                    value=message
                )
                messages_sent += 1
            
            producer.flush()
            
            batch_elapsed = time.time() - batch_start
            if batch_elapsed < 1.0:
                time.sleep(1.0 - batch_elapsed)
            
            elapsed = time.time() - start_time
            actual_rate = messages_sent / elapsed if elapsed > 0 else 0
            print(f"\rMessages sent: {messages_sent:,} | "
                  f"Elapsed: {elapsed:.1f}s | "
                  f"Rate: {actual_rate:.1f} msg/s", end="")
    
    finally:
        producer.flush()
        producer.close()
        
        total_time = time.time() - start_time
        print(f"\n\nGeneration complete!")
        print(f"  Total messages sent: {messages_sent:,}")
        print(f"  Total time: {total_time:.1f} seconds")
        print(f"  Average rate: {messages_sent/total_time:.1f} messages/second")

def main():
    parser = argparse.ArgumentParser(
        description='Generate random IoT sensor data and send to Kafka'
    )
    parser.add_argument(
        '--duration', '-d',
        type=float,
        default=1.0,
        help='Duration to run in minutes (default: 1)'
    )
    parser.add_argument(
        '--rate', '-r',
        type=int,
        default=10,
        help='Messages per second (default: 10)'
    )
    
    args = parser.parse_args()
    
    if args.duration <= 0:
        print("Error: Duration must be positive")
        sys.exit(1)
    if args.rate <= 0:
        print("Error: Rate must be positive")
        sys.exit(1)
    
    run_generator(args.duration, args.rate)

if __name__ == "__main__":
    main()