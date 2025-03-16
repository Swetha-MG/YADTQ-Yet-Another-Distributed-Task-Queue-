import json
import sys
import time
import redis
from kafka import KafkaProducer, KafkaConsumer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TASK_TOPIC = 'tasks'
STATUS_TOPIC = 'task_status_updates'  # Topic for receiving task status updates

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
RESULT_DB = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka Consumer setup for task status updates
consumer = KafkaConsumer(
    STATUS_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id='client_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def load_tasks_from_file(file_path):
    """Load tasks from the provided JSON file."""
    try:
        with open(file_path, 'r') as file:
            tasks = json.load(file)
            return tasks
    except Exception as e:
        print(f"Error loading tasks from file: {e}")
        sys.exit(1)

def send_tasks_to_kafka(tasks):
    """Send tasks to Kafka topic."""
    for task in tasks:
        producer.send(TASK_TOPIC, value=task)
        print(f"Task {task['task-id']} sent to Kafka.")

def listen_for_task_status():
    """Listen for task status updates via Redis Pub/Sub and print them in real-time."""
    print("Listening for task status updates using Redis Pub/Sub...")
    
    # Create a Redis Pub/Sub object
    pubsub = RESULT_DB.pubsub()
    
    # Subscribe to the task status update channel
    pubsub.subscribe('task_status_channel')
    
    # Listen for messages from Redis
    for message in pubsub.listen():
        if message['type'] == 'message':
            status_data = json.loads(message['data'])
            
            task_id = status_data.get('task-id')
            status = status_data.get('status')

            if not task_id or not status:
                print(f"Skipping invalid task status: {status_data}")
                continue

            # Print the task's status
            print(f"Task {task_id} status: {status}")

            if status == 'success':
                result = status_data.get('result')
                print(f"Task {task_id} completed successfully with result: {result}")
            elif status == 'failed':
                error = status_data.get('error')
                print(f"Task {task_id} failed with error: {error}")
            else:
                print(f"Task {task_id} status: {status}")

def publish_task_status(task_id, status, result=None, error=None):
    """Publish task status updates to the Redis channel using Pub/Sub."""
    task_status = {
        'task-id': task_id,
        'status': status
    }
    if result is not None:
        task_status['result'] = result
    if error is not None:
        task_status['error'] = error

    # Publish the task status update to the Redis channel
    RESULT_DB.publish('task_status_channel', json.dumps(task_status))
    print(f"Task {task_id} status published to Redis.")

if __name__ == '__main__':
    # Check if file path is provided
    if len(sys.argv) < 2:
        print("Usage: python client.py <path_to_task_file>")
        sys.exit(1)

    file_path = sys.argv[1]

    # Load tasks from file and send to Kafka
    tasks = load_tasks_from_file(file_path)
    send_tasks_to_kafka(tasks)
    print(f"All tasks from {file_path} have been sent to Kafka.")
    
    # Start listening for task status updates via Redis Pub/Sub
    listen_for_task_status()

