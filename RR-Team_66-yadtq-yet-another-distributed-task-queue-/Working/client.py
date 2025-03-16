import redis

import time

import json



# Redis configuration

REDIS_HOST = 'localhost'  # Ensure this matches your backend Redis host

REDIS_PORT = 6379         # Ensure this matches your backend Redis port

REDIS_DB = 0              # Default database used by the backend



# Initialize Redis client

r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)



def get_task_status(task_id):

    """

    Fetch the status of a specific task from Redis.



    :param task_id: The ID of the task

    :return: A dictionary containing task details or None if the task doesn't exist

    """

    try:

        # Fetch the task result using the same key format as in `save_task_result`

        task_key = f'task_result:{task_id}'

        task_data = r.get(task_key)

        

        if task_data:

            # Parse the JSON task result

            task_info = json.loads(task_data)

            return {

                "task_id": task_id,

                "status": task_info.get("status", "Unknown"),

                "result": task_info.get("result", "No result"),

                "timestamp": task_info.get("timestamp", "No timestamp"),

            }

        else:

            return None

    except Exception as e:

        print(f"Error retrieving task {task_id}: {e}")

        return None





def get_all_tasks():

    """

    Retrieve all tasks currently stored in Redis.



    :return: A list of task information dictionaries

    """

    tasks = []

    try:

        # Use the sorted set `task_results` created in the backend to get all task keys

        task_keys = r.zrange("task_results", 0, -1)

        for task_key in task_keys:

            # Extract the task_id from the key

            task_id = task_key.split(':')[1]

            task_info = get_task_status(task_id)

            if task_info:

                tasks.append(task_info)

        return tasks

    except Exception as e:

        print(f"Error retrieving all tasks: {e}")

        return []





def print_task_statuses():

    """

    Periodically print the status of all tasks in Redis.

    """

    while True:

        tasks = get_all_tasks()  # Fetch the current task statuses from Redis



        if tasks:

            print("\n=== Task Statuses ===")

            for task in tasks:

                print(

                    f"Task ID: {task['task_id']}, "

                    f"Status: {task['status']}, "

                    f"Result: {task['result']}, "

                    f"Timestamp: {task['timestamp']}"

                )

        else:

            print("No tasks found or all tasks are complete.")



        # Sleep for a few seconds before checking again

        time.sleep(5)  # Adjust the sleep time as needed (in seconds)





if __name__ == "__main__":

    print_task_statuses()

