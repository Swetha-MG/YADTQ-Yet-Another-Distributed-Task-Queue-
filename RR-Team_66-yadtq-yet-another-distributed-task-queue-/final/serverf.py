import time
import json
import redis
from kafka import KafkaConsumer, KafkaProducer
import threading

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TASK_TOPIC = 'tasks'
HEARTBEAT_TOPIC = 'heartbeats'

# Redis Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
RESULT_DB = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Task Processing Times (example)
TASK_PROCESSING_TIMES = {
    "add": 2,
    "multiply": 5,
    "subtract": 7
}

# Heartbeat Monitoring
HEARTBEAT_TIMEOUT = 10  # Time in seconds before a worker is considered dead

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def monitor_heartbeats():
    worker_last_heartbeat = {}

    while True:
        # Fetch all heartbeats from Redis and process
        heartbeats = RESULT_DB.lrange(HEARTBEAT_TOPIC, 0, -1)
        for heartbeat in heartbeats:
            message = json.loads(heartbeat)
            worker_id = message['worker_id']
            timestamp = message['timestamp']

            if worker_id not in worker_last_heartbeat:
                print(f"New worker {worker_id} detected.")
                RESULT_DB.hset('worker_loads', worker_id, 0)

            # Update worker heartbeat timestamp
            worker_last_heartbeat[worker_id] = timestamp
            RESULT_DB.lrem(HEARTBEAT_TOPIC, 0, heartbeat)

        # Detect and handle failed workers
        current_time = time.time()
        for worker_id, last_heartbeat in list(worker_last_heartbeat.items()):
            if current_time - last_heartbeat > HEARTBEAT_TIMEOUT:
                print(f"Worker {worker_id} has failed. Reassigning their tasks...")
                reassign_worker_tasks(worker_id)
                RESULT_DB.hset('worker_status', worker_id, 'failed')  # Mark worker as failed
                del worker_last_heartbeat[worker_id]

        # Assign tasks to idle workers if any
        assign_task_to_worker()

        time.sleep(HEARTBEAT_TIMEOUT)

def reassign_worker_tasks(worker_id):
    worker_tasks = RESULT_DB.hgetall(f'worker_tasks_{worker_id}')
    if not worker_tasks:
        print(f"No tasks to reassign for Worker {worker_id}")
        return

    for task_id, task_data in worker_tasks.items():
        task = json.loads(task_data)
        print(f"Reassigning task {task['task-id']} to another worker.")
        assign_task_to_worker(task)
        RESULT_DB.hdel(f'worker_tasks_{worker_id}', task_id)

def assign_task_to_worker(task=None):
    """Assign a task to an idle worker."""
    if not task:
        return

    # Get current worker statuses from Redis
    worker_status = RESULT_DB.hgetall('worker_status')
    print(f"Current worker statuses: {worker_status}")  # Debug output

    # Find all idle workers
    idle_workers = [worker for worker, status in worker_status.items() if status == 'idle']
    if not idle_workers:
        print("No idle workers available.")
        return

    # Find the worker with the least load
    worker_loads = RESULT_DB.hgetall('worker_loads')
    least_loaded_worker = None
    least_load = float('inf')

    for worker in idle_workers:
        load = int(worker_loads.get(worker, 0))  # Default to 0 if no load exists
        if load < least_load:
            least_load = load
            least_loaded_worker = worker

    if least_loaded_worker:
        # Get task type and its processing time
        task_type = task['task']
        task_duration = TASK_PROCESSING_TIMES.get(task_type, 0)

        # Send task to least loaded worker
        producer.send(TASK_TOPIC, value=task, key=str(least_loaded_worker).encode('utf-8'))
        print(f"Task {task['task-id']} ({task_type}, {task_duration}s) sent to worker {least_loaded_worker}")

        # Update worker's status and load
        RESULT_DB.hset('worker_status', least_loaded_worker, 'busy')  # Mark as busy
        RESULT_DB.hincrby('worker_loads', least_loaded_worker, task_duration)  # Increase load
        RESULT_DB.hset(f'worker_tasks_{least_loaded_worker}', task['task-id'], json.dumps(task))
    else:
        print("No suitable worker found.")


def start_heartbeat_listener():
    """Listen to the heartbeats of workers."""
    consumer = KafkaConsumer(
        HEARTBEAT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id='server_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        heartbeat = message.value
        worker_id = heartbeat['worker_id']
        timestamp = heartbeat['timestamp']
        status = heartbeat['status']
        RESULT_DB.rpush(HEARTBEAT_TOPIC, json.dumps(heartbeat))  # Add the heartbeat to Redis
        print(f"Received heartbeat from Worker {worker_id} at {timestamp} (Status: {status})")

if __name__ == '__main__':
    # Start heartbeat listener thread
    heartbeat_listener_thread = threading.Thread(target=start_heartbeat_listener, daemon=True)
    heartbeat_listener_thread.start()

    # Start heartbeat monitoring in the main thread
    monitor_heartbeats()

