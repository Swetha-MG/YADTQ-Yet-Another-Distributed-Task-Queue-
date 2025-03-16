import logging

import json

import time

import random

import threading

from kafka import KafkaConsumer, KafkaProducer

from ..Backend.backend import save_task_result

#from ..core.heartbeat import log_heartbeat_periodically

print("Running consumer_.py")


# Kafka configuration

bootstrap_servers = 'localhost:9092'

task_topic = 'task_topic'

result_topic = 'result_topic'

consumer_group = 'task_consumer_group'



# Configure logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)



# Set Kafka's logging level to WARNING to reduce verbosity

logging.getLogger('kafka').setLevel(logging.WARNING)



# Create Kafka consumer and producer

consumer = KafkaConsumer(

    task_topic,

    bootstrap_servers=bootstrap_servers,

    group_id=consumer_group,

    auto_offset_reset='earliest'

)



producer = KafkaProducer(bootstrap_servers=bootstrap_servers)



# Generate a random worker ID (can be any unique identifier)

worker_id = random.randint(1000, 9999)



# Heartbeat logging function (runs in a separate thread)

def start_heartbeat(worker_id, interval=30):

    def log_heartbeat_periodically():

        while True:

            logger.info(f"Heartbeat from worker {worker_id}: still alive...")

            time.sleep(interval)  # Wait for the next heartbeat interval



    # Start the heartbeat logging thread

    threading.Thread(target=log_heartbeat_periodically, daemon=True).start()



# Start the heartbeat thread

start_heartbeat(worker_id)



# Process tasks

try:

    for message in consumer:

        task = json.loads(message.value.decode('utf-8'))

        task_id = task.get('task-id')

        operation = task.get('task')

        args = task.get('args', [])



        # Log and update the task status in Redis (queued)

        logger.info(f"Received task: {task_id}, Operation: {operation}, Args: {args}")

        save_task_result(task_id, "queued", None)  # Task is queued, no result yet



        # Initialize status and result

        status = "processing"

        result = None



        # Simulate random delay for each operation (between 2 and 5 seconds)

        delay = random.randint(2, 5)

        logger.info(f"Estimating execution time of {delay} seconds for task {task_id}...")

        time.sleep(delay)  # Simulate a processing delay



        try:

            save_task_result(task_id, status, None)  # Update to "processing" in Redis



            # Add failure simulation logic here to simulate worker failure

            if random.random() < 0.1:  # 10% chance to simulate worker failure

                raise Exception(f"Task {task_id} failed unexpectedly!")



            # Perform the actual task based on operation type

            if operation == "add" and len(args) == 2:

                result = sum(args)

                status = "success"

            elif operation == "sub" and len(args) == 2:

                result = args[0] - args[1]

                status = "success"

            elif operation == "multiply" and len(args) == 2:

                result = args[0] * args[1]

                status = "success"

            else:

                status = "fail"

                result = f"Invalid task or arguments for operation: {operation}"



        except Exception as e:

            status = "fail"

            result = f"Error processing task: {e}"

            logger.error(f"Task {task_id} processing failed due to: {e}")



            # Retry mechanism for failed tasks

            retry_count = 3

            for attempt in range(retry_count):

                logger.info(f"Retrying task {task_id}, attempt {attempt + 1}/{retry_count}...")

                time.sleep(random.randint(2, 5))  # Simulate retry delay

                try:

                    # Reattempt the task with same operation and args

                    if operation == "add" and len(args) == 2:

                        result = sum(args)

                        status = "success"

                        break

                    elif operation == "sub" and len(args) == 2:

                        result = args[0] - args[1]

                        status = "success"

                        break

                    elif operation == "multiply" and len(args) == 2:

                        result = args[0] * args[1]

                        status = "success"

                        break

                except Exception as retry_error:

                    logger.error(f"Retry failed for task {task_id} on attempt {attempt + 1}: {retry_error}")



            # If retry count exceeds and task still fails, keep status as "fail"

            if status != "success":

                result = f"Task failed after {retry_count} retries."



        # Log the processed result

        logger.info(f"Processed task: {task_id} --> Result: {result}")



        # Save the result to Redis (backend component) with status update

        save_task_result(task_id, status, result)



        # Send the result to the result topic (serialize it before sending)

        message = json.dumps({'task-id': task_id, 'status': status, 'result': result})

        producer.send(result_topic, message.encode('utf-8'))

        producer.flush()



        logger.info(f"Result for task {task_id} sent to {result_topic}")



except KeyboardInterrupt:

    logger.info("Consumer interrupted. Shutting down...")

finally:

    consumer.close()

    producer.close()

