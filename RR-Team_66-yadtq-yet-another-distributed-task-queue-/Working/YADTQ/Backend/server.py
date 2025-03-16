import redis
from datetime import datetime
import time
from YADTQ.Backend.backend import check_heartbeat  # Assuming you have this function in the heartbeat.py file
from YADTQ.core.heartbeat import check_heartbeat

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Create a Redis client
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Function to get the status of all workers
def get_worker_status(timeout=60):
    worker_statuses = {}
    for key in redis_client.scan_iter("heartbeat:*"):
        worker_id = key.decode('utf-8').split(":")[1]
        last_heartbeat = redis_client.get(key)
        if last_heartbeat:
            last_heartbeat_time = datetime.fromisoformat(last_heartbeat.decode('utf-8'))
            elapsed_time = (datetime.now() - last_heartbeat_time).total_seconds()
            # Mark worker as alive if the elapsed time is less than timeout; else, mark as dead
            worker_statuses[worker_id] = "alive" if elapsed_time <= timeout else "dead"
    return worker_statuses

# Function to monitor and print worker statuses periodically
def monitor_workers(interval=30, timeout=60):
    while True:
        worker_statuses = {}
        for key in redis_client.scan_iter("heartbeat:*"):
            worker_id = key.decode('utf-8').split(":")[1]
            status = check_heartbeat(worker_id, timeout)
            worker_statuses[worker_id] = "alive" if status else "dead"
            # Log the status of the worker
            logger.info(f"Heartbeat status for {worker_id}: {'alive' if status else 'dead'}")
        
        # Display statuses in the console
        print("\nWorker Statuses:")
        for worker, status in worker_statuses.items():
            print(f"{worker}: {status}")
        
        time.sleep(interval)  # Monitor workers periodically


# Function to update the heartbeat of workers (called periodically)
def update_heartbeat(worker_id):
    # Store the current timestamp in Redis as a heartbeat for the worker
    redis_client.set(f"heartbeat:{worker_id}", datetime.now().isoformat())

if __name__ == "__main__":
    # Example worker ID (you can customize this based on your worker IDs)
    worker_id = "worker_1"

    # Monitor workers every 30 seconds with a timeout of 60 seconds
    monitor_workers(interval=30, timeout=60)

    # Periodically update heartbeat for the worker every 5 seconds
    while True:
        update_heartbeat(worker_id)
        # Check heartbeat status for the worker (using check_heartbeat function from heartbeat.py)
        heartbeat_status = check_heartbeat(worker_id)
        print(f"Heartbeat status for {worker_id}: {heartbeat_status}")
        time.sleep(5)  # Update the heartbeat every 5 seconds

