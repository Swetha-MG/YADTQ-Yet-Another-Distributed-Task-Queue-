import time
import json
import redis
from kafka import KafkaConsumer, KafkaProducer
import threading
import uuid
import signal
import sys

# Kafka and Redis configuration (as before)
KAFKA_BROKER = 'localhost:9092'
TASK_TOPIC = 'tasks'
HEARTBEAT_TOPIC = 'heartbeats'
WORKER_ID = 'worker_1'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
RESULT_DB = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
WORKER_TASKS_KEY = f"worker_tasks_{WORKER_ID}"

# Task functions and processing times (same as before)
TASK_FUNCTIONS = {
    'add': lambda args: sum(args),
    'sub': lambda args: args[0] - args[1],
    'multiply': lambda args: args[0] * args[1],
}

TASK_PROCESSING_TIMES = {
    'add': 2,
    'sub': 5,
    'multiply': 7,
}

def publish_worker_status(worker_id, status):
    """Publish worker status to Redis (idle or busy)."""
    try:
        RESULT_DB.hset('worker_status', worker_id, status)
        print(f"Worker {worker_id} status updated to {status}.")
    except Exception as e:
        print(f"Failed to update worker status: {str(e)}")

def publish_task_status(task_id, status, result=None, error=None):
    """Publish task status updates to Redis Pub/Sub channel."""
    task_status = {'task-id': task_id, 'status': status}
    if result is not None:
        task_status['result'] = result
    if error is not None:
        task_status['error'] = error
    RESULT_DB.publish('task_status_channel', json.dumps(task_status))
    print(f"Task {task_id} status published.")

def process_task(task):
    """Process the task received from Kafka."""
    task_id = task['task-id']
    task_type = task['task']
    args = task.get('args', [])

    RESULT_DB.hset(WORKER_TASKS_KEY, task_id, json.dumps(task))
    publish_worker_status(WORKER_ID, 'busy')

    print(f"Worker {WORKER_ID} processing task {task_id}: {task_type}({args})")

    time.sleep(1)  # Simulate delay before processing starts

    try:
        publish_task_status(task_id, 'processing')
        processing_time = TASK_PROCESSING_TIMES[task_type]
        print(f"Processing task {task_id} for {processing_time}s...")
        time.sleep(processing_time)  # Simulate task processing

        result = TASK_FUNCTIONS[task_type](args)
        publish_task_status(task_id, 'success', result=result)
        print(f"Task {task_id} completed with result: {result}")
    except Exception as e:
        publish_task_status(task_id, 'failed', error=str(e))
        print(f"Task {task_id} failed: {str(e)}")
    finally:
        RESULT_DB.hdel(WORKER_TASKS_KEY, task_id)
        publish_worker_status(WORKER_ID, 'idle')

def send_heartbeat():
    """Send heartbeat to indicate worker status."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    try:
        while True:
            worker_status = 'occupied' if RESULT_DB.hlen(WORKER_TASKS_KEY) > 0 else 'idle'
            heartbeat_message = {'worker_id': WORKER_ID, 'status': worker_status, 'timestamp': time.time()}
            producer.send(HEARTBEAT_TOPIC, heartbeat_message)
            print(f"Worker {WORKER_ID} sent heartbeat (status: {worker_status}).")
            time.sleep(5)
    except Exception as e:
        print(f"Heartbeat thread error: {str(e)}")
    finally:
        producer.close()

def handle_exit(signal, frame):
    """Graceful shutdown handler."""
    print(f"Worker {WORKER_ID} shutting down...")
    RESULT_DB.delete(WORKER_TASKS_KEY)
    sys.exit(0)

def worker_loop():
    """The main worker loop consuming tasks from Kafka."""
    consumer = KafkaConsumer(TASK_TOPIC, bootstrap_servers=KAFKA_BROKER, group_id='worker_group', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for message in consumer:
        task = message.value
        process_task(task)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    print(f"Worker {WORKER_ID} started.")
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    worker_loop()

