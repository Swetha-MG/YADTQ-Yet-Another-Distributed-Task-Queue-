import time
import json
import redis
from kafka import KafkaConsumer, KafkaProducer
import threading
import uuid
import signal
import sys

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TASK_TOPIC = 'tasks'
HEARTBEAT_TOPIC = 'heartbeats'
WORKER_ID = str(uuid.uuid4())

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
RESULT_DB = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Worker-specific Redis key for tracking tasks
WORKER_TASKS_KEY = f"worker_tasks_{WORKER_ID}"

# Task functions
TASK_FUNCTIONS = {
    'add': lambda args: sum(args),
    'sub': lambda args: args[0] - args[1],
    'multiply': lambda args: args[0] * args[1],
}

# Task-specific processing times
TASK_PROCESSING_TIMES = {
    'add': 2,
    'sub': 5,
    'multiply': 7,
}

def publish_worker_status(worker_id, status):
    """
    Publish or update the worker's status in Redis.
    :param worker_id: The unique ID of the worker.
    :param status: The status of the worker ('idle' or 'busy').
    """
    try:
        # Update the worker status in Redis
        RESULT_DB.hset('worker_status', worker_id, status)
        print(f"Worker {worker_id} status updated to {status} in Redis.")
    except Exception as e:
        print(f"Failed to update worker status: {str(e)}")

def publish_task_status(task_id, status, result=None, error=None):
    """Publish task status updates to Redis Pub/Sub channel."""
    task_status = {
        'task-id': task_id,  # Always include task-id
        'status': status
    }
    if result is not None:
        task_status['result'] = result
    if error is not None:
        task_status['error'] = error

    # Publish task status to the Redis Pub/Sub channel
    RESULT_DB.publish('task_status_channel', json.dumps(task_status))
    print(f"Task {task_id} status published to Redis Pub/Sub channel.")

def process_task(task):
    task_id = task['task-id']
    task_type = task['task']
    args = task.get('args', [])

    RESULT_DB.hset(WORKER_TASKS_KEY, task_id, json.dumps(task))
    publish_worker_status(WORKER_ID, 'busy')  # Mark worker as busy

    print(f"Worker {WORKER_ID} received task {task_id}: {task_type}({args})")

    # Update Redis status to 'queued'
    publish_task_status(task_id, 'queued')

    time.sleep(1)  # Simulate delay before processing begins

    try:
        # Update status to 'processing'
        publish_task_status(task_id, 'processing')

        if task_type not in TASK_FUNCTIONS or task_type not in TASK_PROCESSING_TIMES:
            raise ValueError(f"Unknown task type: {task_type}")

        processing_time = TASK_PROCESSING_TIMES[task_type]
        print(f"Processing task {task_id} (time: {processing_time}s)...")
        time.sleep(processing_time)

        # Process the task
        result = TASK_FUNCTIONS[task_type](args)

        # Update status to 'success'
        publish_task_status(task_id, 'success', result=result)
        print(f"Task {task_id} completed successfully with result {result}")

    except Exception as e:
        publish_task_status(task_id, 'failed', error=str(e))
        print(f"Task {task_id} failed: {str(e)}")

    finally:
        RESULT_DB.hdel(WORKER_TASKS_KEY, task_id)
        publish_worker_status(WORKER_ID, 'idle')  # Mark worker as idle

# Send heartbeat to the broker
def send_heartbeat():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    try:
        while True:
            # Check worker status (occupied if processing task, idle otherwise)
            worker_status = 'occupied' if RESULT_DB.hlen(WORKER_TASKS_KEY) > 0 else 'idle'
            
            heartbeat_message = {
                'worker_id': WORKER_ID,
                'status': worker_status,
                'timestamp': time.time()
            }
            producer.send(HEARTBEAT_TOPIC, heartbeat_message)
            print(f"Worker {WORKER_ID} sent heartbeat (status: {worker_status}).")
            time.sleep(5)  # Heartbeat interval
    except Exception as e:
        print(f"Heartbeat thread encountered an error: {str(e)}")
    finally:
        producer.close()

# Graceful shutdown handler
def handle_exit(signal, frame):
    print(f"Worker {WORKER_ID} shutting down...")
    # Remove all active tasks from Redis for this worker
    RESULT_DB.delete(WORKER_TASKS_KEY)
    sys.exit(0)

# Worker loop
def worker_loop():
    consumer = KafkaConsumer(
        TASK_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id='worker_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        task = message.value
        process_task(task)

# Start the worker
if __name__ == '__main__':
    # Handle SIGINT and SIGTERM for graceful shutdown
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    print(f"Worker {WORKER_ID} started.")

    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

    # Start worker loop
    worker_loop()
