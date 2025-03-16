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
    """Listen for task status updates and print them in real-time."""
    print("Listening for task status updates...")
    task_ids = set()  # Track task ids to avoid duplicate status processing

    while True:
        # Fetch all task statuses from Redis
        task_statuses = RESULT_DB.hgetall('task_status')
        
        # Process all the task statuses
        for task_id, status_json in task_statuses.items():
            status_data = json.loads(status_json)  # Corrected variable name
            task_id = status_data.get('task-id')  # Use status_data here
            status = status_data.get('status')   # Use status_data here

            # Avoid reprocessing the same task
            if task_id not in task_ids:
                task_ids.add(task_id)  # Mark this task as processed

                # Print the task's status
                if status == 'success':
                    result = status_data.get('result')
                    print(f"Task {task_id} status: {status}, result: {result}")
                elif status == 'failed':
                    error = status_data.get('error')
                    print(f"Task {task_id} status: {status}, error: {error}")
                else:
                    print(f"Task {task_id} status: {status}")

        # Sleep to avoid constant polling and reduce CPU usage
        time.sleep(1)

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
    
    # Start listening for task status updates in real-time
    listen_for_task_status()

