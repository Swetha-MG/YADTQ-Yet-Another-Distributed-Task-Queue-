# core/broker.py
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import uuid
from core.worker_manager import WorkerManager
import time
import random

class KafkaBroker:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.worker_manager = WorkerManager()
        
        # Create separate topics for each worker
        self.bootstrap_servers = bootstrap_servers

    def send_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        
        # Wait for an available worker
        while True:
            available_workers = self.worker_manager.get_available_workers()
            if available_workers:
                # Choose worker with least tasks
                chosen_worker = random.choice(available_workers)
                break
            time.sleep(1)  # Wait before checking again
            
        # Create task
        task = {
            'task_id': task_id,
            'task': task_type,
            'args': args,
            'assigned_worker': chosen_worker
        }
        
        # Send to worker's topic
        self.producer.send(f'tasks_{chosen_worker}', task)
        self.producer.flush()
        
        return task_id, chosen_worker

    def get_consumer(self, worker_id):
        return KafkaConsumer(
            f'tasks_{worker_id}',
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=f'worker_group_{worker_id}'
        )
