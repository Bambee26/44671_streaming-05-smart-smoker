"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Bambee Garfield
    Date: May 21st, 2024

"""

import pika
import sys
import webbrowser
import csv

# Configure Logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Read smoker temps from csv and send to RabbitMQ server
def read_and_send_smoker_temps_from_csv(file_path: str, host: str, queue_name: str):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader: 
            message = f"Time: {row['Time (UTC)']}, Channel1: {row['Channel1']}, Channel2: {row['Channel2']}, Channel3: {row['Channel3']}"
            send_message(host, queue_name, message)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    file_name = 'smoker-temps.csv'  # Change file name to smoker-temps.csv
    host = "localhost"
    queue_name = "smoker_temps_queue"  # Change queue name if needed
    read_and_send_smoker_temps_from_csv(file_name, host, queue_name)