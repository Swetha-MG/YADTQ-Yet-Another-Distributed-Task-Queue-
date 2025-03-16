import json
import redis
import os
import random
import time
from time import sleep

HEARTBEAT_INTERVAL = 5  # Time interval (in seconds) for sending heartbeats
REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)

def connect_redis():
    while True:
        try:
            client = redis.StrictRedis(host='localhost', port=6379, db=0)
            client.ping()
            print("Connected to Redis")
            return client
        except redis.exceptions.ConnectionError as e:
            print(f"Redis connection error: {e}. Retrying in 5 seconds...")
            sleep(5)

# Function to save results to a JSON file
RESULTS_FILE = "task_results.json"

def save_results_to_file(task_result):
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'r') as file:
            results = json.load(file)
    else:
        results = {}

    task_id = task_result['task_id']
    results[task_id] = task_result

    with open(RESULTS_FILE, 'w') as file:
        json.dump(results, file, indent=4)
    print(f"Task result for {task_id} saved to {RESULTS_FILE}")

def process_task(task):
    try:
        task_id = task["task_id"]
        task_type = task["task_type"]
        args = task["args"]
        if task_type == "add":
            result = sum(args)
        elif task_type == "sub":
            result = args[0] - args[1]
        elif task_type == "multiply":
            result = args[0] * args[1]
        else:
            result = "Unknown task type"

        sleep(random.uniform(1, 3))  # Simulate processing time
        return result
    except Exception as e:
        print(f"Error processing task {task_id}: {e}")
        return "Error"

def send_heartbeat(worker_id):
    """Send heartbeat to Redis to indicate that the worker is alive."""
    REDIS_CLIENT.set(f"worker:{worker_id}:heartbeat", time.time())

def worker_loop():
    worker_id = os.environ.get("WORKER_ID", f"worker_{random.randint(1, 1000)}")
    print(f"Worker {worker_id} started and is ready to process tasks.")
    
    while True:
        # Send heartbeat periodically
        send_heartbeat(worker_id)
        
        task = REDIS_CLIENT.rpop('task_queue')
        if task:
            task = json.loads(task)
            print(f"Worker {worker_id} is processing task {task['task_id']}")
            result = process_task(task)

            # Prepare the result data
            task_result = {
                "task_id": task["task_id"],
                "status": "success" if result != "Error" else "failed",
                "result": result
            }

            # Update task status in Redis
            REDIS_CLIENT.hset(task["task_id"], mapping={
                "status": task_result["status"],
                "result": task_result["result"]
            })

            # Save result to a JSON file
            save_results_to_file(task_result)

            # Also display the result in the terminal
            print(f"Worker {worker_id} completed task {task['task_id']} with result: {result}")
        else:
            print(f"Worker {worker_id} is idle, no task found.")
            sleep(2)  # Wait for a moment before polling again

if __name__ == "__main__":
    worker_loop()
