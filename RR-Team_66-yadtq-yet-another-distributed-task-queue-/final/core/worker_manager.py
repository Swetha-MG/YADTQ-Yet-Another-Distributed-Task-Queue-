# core/worker_manager.py
import redis
import json
import time
from datetime import datetime, timedelta

class WorkerManager:
    def __init__(self, host='localhost', port=6379, db=1):
        self.redis_client = redis.Redis(host=host, port=port, db=db)
        self.heartbeat_timeout = 10  # seconds

    def register_worker(self, worker_id, status='idle'):
        worker_data = {
            'status': status,
            'last_heartbeat': datetime.now().isoformat(),
            'task_count': 0
        }
        self.redis_client.hmset(f'worker:{worker_id}', worker_data)

    def update_worker_status(self, worker_id, status, task_id=None):
        self.redis_client.hset(f'worker:{worker_id}', 'status', status)
        if task_id:
            self.redis_client.hset(f'worker:{worker_id}', 'current_task', task_id)
        self.redis_client.hset(f'worker:{worker_id}', 'last_heartbeat', datetime.now().isoformat())

    def update_heartbeat(self, worker_id):
        self.redis_client.hset(f'worker:{worker_id}', 'last_heartbeat', datetime.now().isoformat())

    def increment_task_count(self, worker_id):
        self.redis_client.hincrby(f'worker:{worker_id}', 'task_count', 1)

    def get_worker_status(self, worker_id):
        data = self.redis_client.hgetall(f'worker:{worker_id}')
        return {k.decode(): v.decode() for k, v in data.items()} if data else None

    def get_available_workers(self):
        workers = []
        for key in self.redis_client.keys('worker:*'):
            worker_id = key.decode().split(':')[1]
            worker_data = self.get_worker_status(worker_id)
            
            if worker_data:
                last_heartbeat = datetime.fromisoformat(worker_data['last_heartbeat'])
                if datetime.now() - last_heartbeat < timedelta(seconds=self.heartbeat_timeout):
                    if worker_data['status'] == 'idle':
                        workers.append(worker_id)
        
        return workers

    def get_worker_stats(self):
        stats = []
        for key in self.redis_client.keys('worker:*'):
            worker_id = key.decode().split(':')[1]
            worker_data = self.get_worker_status(worker_id)
            if worker_data:
                stats.append({
                    'worker_id': worker_id,
                    'status': worker_data['status'],
                    'task_count': int(worker_data.get('task_count', 0)),
                    'last_heartbeat': worker_data['last_heartbeat']
                })
        return stats
