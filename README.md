🧾 Project Summary: YADTQ – Yet Another Distributed Task Queue
🔧 What It Does:
YADTQ is a simple distributed task queue system that lets users submit tasks to be processed by multiple worker machines in parallel. It mimics how job queues in a lightweight, socket-based way.

🧠 How It Works (Overview):
Producer (producer.py)
Sends tasks (like work1, work2, etc.) to the central dispatcher.

Dispatcher (dispatcher.py)
Acts as the brain of the system. It:

Listens for new tasks from producers.

Queues incoming tasks.

Sends tasks to available workers.

Workers (worker.py)
Each worker:

Listens on a specific port.

Waits for tasks from the dispatcher.

Executes the task by dynamically running a corresponding script (work1.py, work2.py, etc.).

Work Files (work1.py, work2.py, work3.py)
These are simple Python scripts with a run() function, simulating different types of jobs.


📦 Technologies Used:

Python Sockets (socket)

Threads (threading)

Modular task execution using dynamic imports
