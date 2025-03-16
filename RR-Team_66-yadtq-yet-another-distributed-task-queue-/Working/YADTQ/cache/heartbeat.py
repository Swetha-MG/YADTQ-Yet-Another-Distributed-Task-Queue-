import redis
import time
from datetime import datetime

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0  # Default database

# Create a Redis connection
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def log_heartbeat_periodically(worker_id):
    while True:
        # Update the worker's heartbeat timestamp in Redis
        heartbeat_key = f"heartbeat:{worker_id}"
        current_time = datetime.now().isoformat()  # Use ISO 8601 format for timestamps
        redis_client.set(heartbeat_key, current_time)

        # Log heartbeats
        logger.info(f"Heartbeat sent for worker {worker_id}: {current_time}")

        # Send heartbeat to Kafka topic
        producer.send('heartbeat_topic', json.dumps({'worker_id': worker_id, 'timestamp': current_time}).encode('utf-8'))

        time.sleep(interval)  # Wait for the next heartbeat interval

    
def check_heartbeat(worker_id, timeout=60):
    """
    Check the worker's last heartbeat to see if it has timed out.
    
    :param worker_id: Unique ID of the worker
    :param timeout: Time in seconds after which the worker is considered dead if no heartbeat is found
    :return: True if the worker is alive, False if the worker has timed out
    """
    try:
        last_heartbeat = redis_client.get(f"heartbeat:{worker_id}")
        if last_heartbeat:
            last_heartbeat_time = datetime.fromisoformat(last_heartbeat.decode('utf-8'))
            elapsed_time = (datetime.now() - last_heartbeat_time).total_seconds()
            if elapsed_time > timeout:
                return False  # Worker has timed out
            return True  # Worker is alive
        return False  # No heartbeat found, worker is considered dead
    except Exception as e:
        print(f"Error checking heartbeat for worker {worker_id}: {e}")
        return False  # In case of error, consider the worker as dead

# Example usage (for worker 1)
worker_id = 'worker_1'

# Log heartbeat every 30 seconds (for example)
while True:
    log_heartbeat_periodically(worker_id)
    time.sleep(30)  # Sleep for 30 seconds before logging next heartbeat

# In a separate process or thread, you can periodically check heartbeat

