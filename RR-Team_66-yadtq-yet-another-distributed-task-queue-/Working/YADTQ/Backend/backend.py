import redis
import json
import logging
from datetime import datetime

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0  # Default database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Redis connection
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def save_task_result(task_id, status, result=None):
    try:
        if task_id is None or status is None:
            raise ValueError(f"Invalid task_id or status: task_id={task_id}, status={status}")

        # Log before saving
        logger.info(f"Saving task result: task_id={task_id}, status={status}, result={result}")

        task_result = {
            "task-id": task_id,
            "status": status,
            "result": result if result is not None else "N/A",
            "timestamp": datetime.now().isoformat(),
        }
        redis_client.set(f'task_result:{task_id}', json.dumps(task_result))
        redis_client.zadd('task_results', {f'task_result:{task_id}': datetime.now().timestamp()})

        logger.info(f"Task {task_id} saved successfully.")
    except Exception as e:
        logger.error(f"Error saving task result for {task_id}: {e}")
        raise  # Re-raise for debugging if necessary


def get_task_result(task_id):
    """
    Retrieve the task result from Redis.

    :param task_id: The ID of the task
    :return: The task result or None if not found
    """
    try:
        # Get the task result from Redis
        task_result = redis_client.get(f'task_result:{task_id}')
        if task_result:
            logger.info(f"Task result for {task_id} retrieved successfully.")
            return json.loads(task_result)
        else:
            logger.warning(f"No task result found for {task_id}.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving task result for {task_id}: {e}")
        return None

