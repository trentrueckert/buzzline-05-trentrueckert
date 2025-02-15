"""
utils_consumer.py - common functions used by consumers.

Consumers subscribe to a topic and read messages from the Kafka topic.
"""

#####################################
# Imports
#####################################


# Import external packages
from kafka import KafkaConsumer

# Import functions from local modules
from .utils_config import get_kafka_broker_address
from .utils_logger import logger


#####################################
# Helper Functions
#####################################


def create_kafka_consumer(
    topic_provided: str = None,
    group_id_provided: str = None,
    value_deserializer_provided=None,
):
    """
    Create and return a Kafka consumer instance.

    Args:
        topic_provided (str): The Kafka topic to subscribe to. Defaults to the environment variable or default.
        group_id_provided (str): The consumer group ID. Defaults to the environment variable or default.
        value_deserializer_provided (callable, optional): Function to deserialize message values.

    Returns:
        KafkaConsumer: Configured Kafka consumer instance.
    """
    kafka_broker = get_kafka_broker_address()
    topic = topic_provided
    consumer_group_id = group_id_provided or "test_group"
    logger.info(
        f"Creating Kafka consumer. Topic='{topic}' and group ID='{group_id_provided}'."
    )
    logger.debug(f"Kafka broker: {kafka_broker}")

    try:
        consumer = KafkaConsumer(
            topic,
            group_id=consumer_group_id,
            value_deserializer=value_deserializer_provided
            or (lambda x: x.decode("utf-8")),
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        logger.info("Kafka consumer created successfully.")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise

def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message.
    Counts category occurrences and extracts necessary fields for storage.

    Args:
        message (dict): The JSON message as a Python dictionary.

    Returns:
        dict: The processed message with extracted data.
    """
    logger.info("Processing message: %s", message)

    try:
        # Define the categories you want to track and count
        CATEGORIES = ["humor", "tech", "food", "travel", "entertainment", "gaming"]
        message_text = message.get("message", "").lower()

        # Count occurrences of categories in the message text
        category_counts = {category: message_text.count(category) for category in CATEGORIES}

        # Create a processed message with necessary fields
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
            "humor_count": int(category_counts["humor"]),
            "tech_count": int(category_counts["tech"]),
            "food_count": int(category_counts["food"]),
            "travel_count": int(category_counts["travel"]),
            "entertainment_count": int(category_counts["entertainment"]),
            "gaming_count": int(category_counts["gaming"]),
        }

        logger.info(f"Processed message: {processed_message}")
        return processed_message

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None
