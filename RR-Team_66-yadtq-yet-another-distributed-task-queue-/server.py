import subprocess
import time
import os

def start_worker(worker_id):
    env = os.environ.copy()
    env["WORKER_ID"] = worker_id
    return subprocess.Popen(["python3", "worker.py"], env=env)


# Function to stop all workers
def stop_workers(workers):
    for worker in workers:
        worker.terminate()

# Main server loop
if __name__ == "__main__":
    num_workers = 3  # Number of worker processes
    workers = []
    for i in range(num_workers):
        worker_id = f"worker_{i + 1}"  # Assign a unique ID to each worker
        workers.append(start_worker(worker_id))  # Start worker with the specified ID
        print(f"Started worker {worker_id}")

    print(f"Started {num_workers} worker processes.")

    try:
        while True:
            # Keep the server running and check on worker status if needed
            time.sleep(10)
    except KeyboardInterrupt:
        print("Shutting down workers...")
        stop_workers(workers)
        print("All workers have been stopped.")
