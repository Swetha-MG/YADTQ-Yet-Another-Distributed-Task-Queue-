import redis
import json
from uuid import uuid4
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def load_tasks_from_json(file_path):
    with open(file_path, 'r') as file:
        tasks = json.load(file)
    return tasks

def submit_task(task_type, args, task_id):
    redis_client.hset(task_id, "status", "queued")
    
    task = {
        "task_id": task_id,
        "task_type": task_type,
        "args": args
    }
    redis_client.rpush("task_queue", json.dumps(task))
    print(f"Task {task_id} submitted with status 'queued'")

def check_task_status(task_id):
    status = redis_client.hget(task_id, "status").decode("utf-8")
    result = redis_client.hget(task_id, "result")
    if result:
        result = result.decode("utf-8")
    return {"status": status, "result": result}

def monitor_tasks(task_ids):
    while task_ids:
        for task_id in task_ids:
            status_info = check_task_status(task_id)

            if status_info["status"] in ["success", "failed"]:
                print(f"Task {task_id} Status: {status_info['status']}, Result: {status_info['result']}")
                task_ids.remove(task_id)

        time.sleep(2)

if __name__ == "__main__":
    # Load tasks from the JSON file
    tasks = load_tasks_from_json('tasks.json')
    task_ids = []

    for task in tasks:
        task_id = task['task_id']
        task_type = task['task_type']
        args = task['args']
        submit_task(task_type, args, task_id)
        task_ids.append(task_id)
        time.sleep(1)  # Simulate time between task submissions

    monitor_tasks(task_ids)
