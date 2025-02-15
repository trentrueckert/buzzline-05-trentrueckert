import json
import os
import pathlib
import sys
from kafka import KafkaConsumer
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available
from utils.utils_consumer import create_kafka_consumer, process_message
from consumers.db_sqlite_case import init_db, insert_message, clear_db
import sqlite3

# Add the clear_db function to handle clearing the database
def clear_db(sql_path: pathlib.Path):
    """
    Clears all data in the 'streamed_messages' table of the SQLite database.
    This function is called before processing new messages to ensure a fresh start.
    
    Args:
        sql_path (pathlib.Path): Path to the SQLite database file.
    """
    try:
        conn = sqlite3.connect(sql_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM streamed_messages")  # Clears all rows from the table
        conn.commit()
        logger.info("Database cleared successfully.")
    except Exception as e:
        logger.error(f"Error clearing the database: {e}")
        raise e
    finally:
        conn.close()


# Function to consume messages from Kafka
def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Clear the database before consuming new messages.")
    try:
        # Clear the existing data from the database before starting fresh
        clear_db(sql_path)  # Clear database before new data insertion
    except Exception as e:
        logger.error(f"ERROR: Failed to clear the database: {e}")
        sys.exit(2)

    logger.info("Step 5. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        # consumer is a KafkaConsumer
        # message is a kafka.consumer.fetcher.ConsumerRecord
        # message.value is a Python dictionary
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                try:
                    insert_message(processed_message, sql_path)
                    logger.info(f"Message stored successfully: {processed_message}")
                except Exception as e:
                    logger.error(f"Error inserting message into SQLite: {e}")
                    continue

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


# Define Main Function
def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


# Conditional Execution
if __name__ == "__main__":
    main()

