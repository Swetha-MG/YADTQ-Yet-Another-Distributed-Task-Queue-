from client import Client
import time
import random
import threading
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_load_test(num_tasks=20):
    client = Client()
    operations = ['add', 'subtract', 'multiply']
    tasks = []
    
    def submit_and_wait(op, args):
        try:
            task_id = client.submit_task(op, args)
            if not task_id:
                logging.error("Failed to get valid task_id during task submission.")
                return
            result = client.wait_for_task(task_id)
            logging.info(f"Task {task_id} completed with result: {result}")
        except Exception as e:
            logging.error(f"Error in submit_and_wait for task {op} with args {args}: {e}")

    # Submit tasks
    threads = []
    for _ in range(num_tasks):
        op = random.choice(operations)
        args = [random.randint(1, 100), random.randint(1, 100)]
        thread = threading.Thread(target=submit_and_wait, args=(op, args))
        threads.append(thread)
        thread.start()
        time.sleep(0.5)  # Small delay between task submissions

    # Wait for all tasks to complete
    for thread in threads:
        thread.join()

    # Print worker statistics
    logging.info("\nWorker Statistics:")
    try:
        stats = client.get_worker_stats()
        for worker in stats:
            logging.info(f"Worker {worker['worker_id']}:")
            logging.info(f"  Status: {worker['status']}")
            logging.info(f"  Tasks Completed: {worker['task_count']}")
            logging.info(f"  Last Heartbeat: {worker['last_heartbeat']}")
    except Exception as e:
        logging.error(f"Error fetching worker statistics: {e}")

if __name__ == "__main__":
    run_load_test()

