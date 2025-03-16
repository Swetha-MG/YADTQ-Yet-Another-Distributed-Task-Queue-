import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def assign_task_to_worker(task):
    # Get current worker loads from Redis
    worker_loads = RESULT_DB.hgetall('worker_loads')
    worker_loads = {int(worker): int(load) for worker, load in worker_loads.items()}
    
    # Log worker loads
    logger.info(f"Current worker loads: {worker_loads}")

    # Get the task type and its processing time
    task_type = task['task']
    task_duration = TASK_PROCESSING_TIMES.get(task_type, 0)

    # Find the least-loaded worker
    least_loaded_worker = min(worker_loads, key=worker_loads.get)

    # Log task assignment
    logger.info(f"Assigning task {task['task-id']} to worker {least_loaded_worker} with load {worker_loads[least_loaded_worker]}")

    # Send the task to the least-loaded worker
    producer.send(TASK_TOPIC, value=task, key=str(least_loaded_worker).encode('utf-8'))
    logger.info(f"Task {task['task-id']} ({task_type}, {task_duration}s) sent to partition {least_loaded_worker}")

    # Update the worker's load in Redis
    RESULT_DB.hincrby('worker_loads', least_loaded_worker, task_duration)

