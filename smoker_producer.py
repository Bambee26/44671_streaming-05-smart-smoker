import pika
import sys
import csv
import time
import webbrowser  # Import webbrowser module for opening URLs

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

RABBITMQ_HOST = "localhost"

def connect_rabbitmq():
    """Connect to RabbitMQ and return connection and channel."""
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        channel = connection.channel()
        return connection, channel
    except Exception as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

def create_and_declare_queues(channel, queues):
    """Delete existing queues and declare new ones."""
    for queue_name in queues:
        channel.queue_delete(queue=queue_name)
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"Queue '{queue_name}' declared.")

# Read smoker temps from csv and send to RabbitMQ server
def read_and_send_smoker_temps_from_csv(file_path: str, host: str, queues: list):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = row['Time (UTC)']
            for index, queue_name in enumerate(queues, start=1):
                temp_str = row.get(f'Channel{index}', '')
                if temp_str:
                    try:
                        temp = float(temp_str)
                        message = f"Time: {timestamp}, Channel{index}: {temp}"
                        send_message(host, queue_name, message)
                    except ValueError as e:
                        logger.error(f"Error converting temperature value to float: {e}")
                else:
                    logger.warning(f"Empty temperature value for {index} at {timestamp}. Skipping.")
            time.sleep(10)  # Sleep for 30 seconds before sending the next message

    # Offer to open the RabbitMQ Admin website
    offer_rabbitmq_admin_site()

def send_message(host: str, queue_name: str, message: str):
    """Send a message to the RabbitMQ queue."""
    connection, channel = connect_rabbitmq()
    try:
        channel.basic_publish(exchange='', routing_key=queue_name, body=message.encode())
        logger.info(f" [x] Sent '{message}' to queue '{queue_name}'.")
    except Exception as e:
        logger.error(f"Error sending message to queue '{queue_name}'.")
        logger.error(f"The error says: {e}")
    finally:
        connection.close()

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website."""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info("Opened RabbitMQ Admin website.")

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    file_name = 'smoker-temps.csv'
    host = "localhost"
    queues = ["01-smoker-temp", "02-roast-temp", "03-ribs-temp"]
    connection, channel = connect_rabbitmq()
    create_and_declare_queues(channel, queues)
    read_and_send_smoker_temps_from_csv(file_name, host, queues)
