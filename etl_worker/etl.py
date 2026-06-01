import paho.mqtt.client as mqtt
import boto3
import json
import datetime
import uuid
import time
import threading

import psycopg2
from psycopg2.extras import execute_values

###### add here database connection

# Configuration
MQTT_BROKER = "mqtt_broker" # Docker service name
MQTT_PORT = 1883
MQTT_TOPIC = "#"            # Subscribe to all topics
MINIO_ENDPOINT = "http://minio:9000"
BUCKET_NAME = "iot-raw-bucket"
BATCH_SIZE = 100            # Flush after 100 messages
FLUSH_INTERVAL = 10         # OR flush after 10 seconds

# Global buffer and lock for thread safety
message_buffer = []
buffer_lock = threading.Lock()
last_flush_time = time.time()

# Setup S3 Client (MinIO)
s3_client = boto3.client('s3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

def flush_buffer():
    """Saves the current buffer to MinIO and clears it."""
    global message_buffer, last_flush_time
    
    with buffer_lock:
        if not message_buffer:
            return
            
        # Copy data and clear global buffer immediately to keep receiving messages
        batch_data = list(message_buffer)
        message_buffer.clear()
        last_flush_time = time.time()

    # Generate Partitioned Object Key (year=YYYY/month=MM/...)
    now = datetime.datetime.now()
    partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    file_name = f"batch_{uuid.uuid4().hex}.json"
    object_key = f"{partition_path}/{file_name}"

    try:
        # Convert batch to a newline-delimited JSON string (standard for data lakes)
        payload = "\n".join(json.dumps(msg) for msg in batch_data)
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=object_key,
            Body=payload
        )
        print(f"Flushed {len(batch_data)} records to MinIO: {object_key}")
    except Exception as e:
        print(f"Failed to write to MinIO: {e}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    """Triggered every time a new sensor message arrives."""
    try:
        # Decode the payload and add topic/timestamp metadata
        payload = json.loads(msg.payload.decode('utf-8'))
        payload['_topic'] = msg.topic
        payload['_ingested_at'] = datetime.datetime.now().isoformat()
        
        with buffer_lock:
            message_buffer.append(payload)
            
    except Exception as e:
        print(f"Error parsing message: {e}")

# Start MQTT Client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Run MQTT listener in a background thread
client.loop_start()

print("ETL Worker started. Listening for data...")

# Main Loop: Check if we need to flush based on size or time
try:
    while True:
        time.sleep(1)
        with buffer_lock:
            buffer_size = len(message_buffer)
            
        time_since_flush = time.time() - last_flush_time
        
        if buffer_size >= BATCH_SIZE or (buffer_size > 0 and time_since_flush >= FLUSH_INTERVAL):
            flush_buffer()
except KeyboardInterrupt:
    print("Shutting down...")
    flush_buffer() # Final flush on exit
    client.loop_stop()