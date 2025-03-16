import time
import json
import threading
import uuid
import os
import signal
import random  # Ensure this is imported for random.uniform
from core.broker import KafkaBroker
from core.result_backend import ResultBackend
from core.worker_manager import WorkerManager

class Worker:
    def __init__(self, worker_id=None):
        self.worker_id = worker_id or str(uuid.uuid4())
        self.broker = KafkaBroker()
        self.result_backend = ResultBackend()
        self.worker_manager = WorkerManager()
        self.consumer = self.broker.get_consumer(self.worker_id)
        self.processed_tasks = set()
        self.stop_event = threading.Event()
        self.current_task = None

    def process_task(self, task):
        try:
            if not task or not isinstance(task, dict):
                raise ValueError("Invalid task format")

            task_id = task.get('task_id')
            if not task_id:
                raise ValueError("Missing task ID")

            if task_id in self.processed_tasks:
                return

            self.processed_tasks.add(task_id)
            self.current_task = task_id

            # Update worker and task status
            self.worker_manager.update_worker_status(self.worker_id, 'busy', task_id)
            self.result_backend.save_task_status(task_id, 'processing', worker_id=self.worker_id)

            # Validate task type and arguments
            task_type = task.get('task')
            args = task.get('args')
            if not args or not isinstance(args, list) or len(args) != 2:
                raise ValueError("Invalid task arguments")

            # Perform the task based on its type
            if task_type == 'add':
                result = args[0] + args[1]
            elif task_type == 'subtract':
                result = args[0] - args[1]
            elif task_type == 'multiply':
                result = args[0] * args[1]
            else:
                raise ValueError(f"Unknown task type: {task_type}")

            # Simulate variable processing time
            time.sleep(random.uniform(1, 5))

            # Save success result
            self.result_backend.save_task_status(task_id, 'success', result, self.worker_id)
            self.worker_manager.increment_task_count(self.worker_id)
            print(f"Worker {self.worker_id}: Task {task_id} completed successfully")

        except Exception as e:
            # Handle and log task failure
            self.result_backend.save_task_status(task_id, 'failed', str(e), self.worker_id)
            print(f"Worker {self.worker_id}: Task {task_id} failed: {e}")

        finally:
            # Clean up task state
            self.current_task = None
            self.worker_manager.update_worker_status(self.worker_id, 'idle')

    def start(self):
        # Register worker
        self.worker_manager.register_worker(self.worker_id)

        def heartbeat():
            while not self.stop_event.is_set():
                self.worker_manager.update_heartbeat(self.worker_id)
                time.sleep(1)

        def task_consumer():
            for message in self.consumer:
                if self.stop_event.is_set():
                    break
                try:
                    # Decode Kafka message
                    task = json.loads(message.value.decode('utf-8'))
                    print(f"Worker {self.worker_id}: Received task {task}")
                    if task.get('assigned_worker') == self.worker_id:
                        self.process_task(task)
                except Exception as e:
                    print(f"Worker {self.worker_id}: Failed to process message: {e}")

        # Start threads for heartbeat and task consumption
        heartbeat_thread = threading.Thread(target=heartbeat)
        consumer_thread = threading.Thread(target=task_consumer)

        heartbeat_thread.start()
        consumer_thread.start()

        def signal_handler(signum, frame):
            print(f"Worker {self.worker_id} shutting down...")
            self.stop_event.set()

        # Handle shutdown signals
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            consumer_thread.join()
            heartbeat_thread.join()
        finally:
            self.worker_manager.update_worker_status(self.worker_id, 'offline')

if __name__ == "__main__":
    worker = Worker()
    print(f"Starting worker with ID: {worker.worker_id}")
    worker.start()

