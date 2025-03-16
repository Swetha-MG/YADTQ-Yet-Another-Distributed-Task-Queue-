from core.broker import KafkaBroker
from core.result_backend import ResultBackend
from core.worker_manager import WorkerManager
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Client:
    def __init__(self):
        self.broker = KafkaBroker()
        self.result_backend = ResultBackend()
        self.worker_manager = WorkerManager()

    def submit_task(self, task_type, args):
        try:
            # Send task to the broker
            task_id, worker_id = self.broker.send_task(task_type, args)
            if not task_id or not worker_id:
                raise ValueError("Received invalid task_id or worker_id from broker.")

            # Save task status
            task_data = {'status': 'queued', 'worker_id': worker_id}
            self.result_backend.save_task_status(task_id, **task_data)
            logging.info(f"Task {task_id} assigned to worker {worker_id}")
            return task_id
        except Exception as e:
            logging.error(f"Failed to submit task: {e}")
            raise

    def get_task_status(self, task_id):
        try:
            return self.result_backend.get_task_status(task_id)
        except Exception as e:
            logging.error(f"Failed to fetch task status for {task_id}: {e}")
            return None

    def wait_for_task(self, task_id, timeout=60):
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                status = self.get_task_status(task_id)
                if status and status.get('status') in ['success', 'failed']:
                    return status
                time.sleep(1)
            return {'status': 'timeout'}
        except Exception as e:
            logging.error(f"Error while waiting for task {task_id}: {e}")
            return {'status': 'error'}

    def get_worker_stats(self):
        try:
            return self.worker_manager.get_worker_stats()
        except Exception as e:
            logging.error(f"Failed to fetch worker stats: {e}")
            return None

