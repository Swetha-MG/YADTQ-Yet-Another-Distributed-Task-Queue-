# core/result_backend.py
import redis
import json
from datetime import datetime

class ResultBackend:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=db)

    def save_task_status(self, task_id, status, result=None, worker_id=None):
    try:
        if not task_id:
            raise ValueError("task_id cannot be None")
        if not status:
            raise ValueError("status cannot be None")
        
        # Prepare task data with validation
        task_data = {
            'status': status,
            'result': result if result is not None else '',
            'worker_id': worker_id if worker_id is not None else ''
        }
        
        # Ensure all values are valid types for Redis
        validated_data = {k: (str(v) if v is not None else '') for k, v in task_data.items()}
        
        # Save to Redis
        self.redis_client.hmset(f'task:{task_id}', validated_data)
    except Exception as e:
        logging.error(f"Error saving task status for task {task_id}: {e}")
        raise


    def get_task_status(self, task_id):
        task_data = self.redis_client.hgetall(f'task:{task_id}')
        return {k.decode(): v.decode() for k, v in task_data.items()} if task_data else None
