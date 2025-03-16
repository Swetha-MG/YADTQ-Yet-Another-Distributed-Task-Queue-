import json
from kafka import KafkaProducer

# Kafka producer configuration
bootstrap_servers = 'localhost:9092'
task_topic = 'task_topic'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize task as JSON
)

# List of tasks to enqueue
tasks = [
    {"task-id": "1", "task": "add", "args": [1, 2]},
    {"task-id": "2", "task": "sub", "args": [10, 5]},
    {"task-id": "3", "task": "multiply", "args": [3, 4]},
    {"task-id": "4", "task": "add", "args": [100, -50]},
    {"task-id": "5", "task": "sub", "args": [15, 15]},
    {"task-id": "6", "task": "multiply", "args": [0, 10]},
    {"task-id": "7", "task": "add", "args": [1]},
    {"task-id": "8", "task": "sub", "args": []},
    {"task-id": "9", "task": "multiply", "args": [2, None]},
    {"task-id": "10", "task": "add", "args": [1e12, 1e12]},
    {"task-id": "11", "task": "sub", "args": [5.5, 2.2]},
    {"task-id": "12", "task": "multiply", "args": [-3, 7]},
    {"task-id": "13", "task": "divide", "args": [10, 2]},
]

# Enqueue tasks
for task in tasks:
    # Enqueue the task to the task topic
    producer.send(task_topic, task)
    print(f"Task enqueued: {task['task-id']}")

# Close the producer connection
producer.close()

