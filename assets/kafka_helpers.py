# Standard Library Imports
import traceback
from typing import List, Optional

# Third-Party Library Imports
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError

# Local Application/Project Imports
from assets.utilities import Result, StatusCode, Utilities


class KafkaController:
    """
    A class to manage Kafka topics, producers, and consumers.
    """

    def __init__(self, utils: Utilities, bootstrap_servers: List[str]):
        """
        Initialize the KafkaController with utility functions and Kafka configurations.

        :param utils: An instance of the Utilities class for logging and helper functions.
        :type utils: Utilities
        :param bootstrap_servers: List of Kafka bootstrap servers.
        :type bootstrap_servers: List[str]
        """
        self.utils = utils
        self.logger = self.utils.logger
        self.bootstrap_servers = bootstrap_servers

        # Initialize Kafka Admin Client
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ) -> Result:
        """
        Create a Kafka topic.

        :param topic_name: The name of the topic to create.
        :type topic_name: str
        :param num_partitions: The number of partitions for the topic. Defaults to 1.
        :type num_partitions: int
        :param replication_factor: The replication factor for the topic. Defaults to 1.
        :type replication_factor: int
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            self.logger.info(
                f"Creating Kafka topic '{topic_name}' with {num_partitions} partitions and replication factor {replication_factor}."
            )

            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            self.admin_client.create_topics([topic])

            return Result(
                StatusCode.SUCCESS, f"Topic '{topic_name}' created successfully."
            )
        except KafkaError as e:
            error_msg = f"Failed to create topic '{topic_name}': {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return Result(StatusCode.FAILURE, error_msg)

    def produce_message(
        self, topic_name: str, message_key: Optional[bytes], message_value: bytes
    ) -> Result:
        """
        Produce a message to a Kafka topic.

        :param topic_name: The name of the topic to produce to.
        :type topic_name: str
        :param message_key: Optional key for the message.
        :type message_key: Optional[bytes]
        :param message_value: The value of the message.
        :type message_value: bytes
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            self.logger.info(f"Producing message to topic '{topic_name}'...")

            producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
            future = producer.send(topic_name, key=message_key, value=message_value)
            record_metadata = future.get(timeout=10)

            self.logger.info(
                f"Message sent to {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}"
            )
            producer.close()

            return Result(StatusCode.SUCCESS, "Message produced successfully.")
        except KafkaError as e:
            error_msg = f"Failed to produce message to topic '{topic_name}': {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return Result(StatusCode.FAILURE, error_msg)

    def consume_messages(
        self, topic_name: str, group_id: str, max_messages: int = 10
    ) -> Result:
        """
        Consume messages from a Kafka topic.

        :param topic_name: The name of the topic to consume from.
        :type topic_name: str
        :param group_id: The consumer group ID.
        :type group_id: str
        :param max_messages: The maximum number of messages to consume. Defaults to 10.
        :type max_messages: int
        :return: A Result object with the consumed messages.
        :rtype: Result
        """
        try:
            self.logger.info(
                f"Consuming up to {max_messages} messages from topic '{topic_name}' (group_id: '{group_id}')..."
            )

            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="earliest",
            )

            messages = []
            for i, message in enumerate(consumer):
                self.logger.info(
                    f"Consumed message: {message.value.decode('utf-8')} from partition {message.partition} at offset {message.offset}"
                )
                messages.append(message.value.decode("utf-8"))
                if i + 1 >= max_messages:
                    break

            consumer.close()

            return Result(
                StatusCode.SUCCESS, "Messages consumed successfully.", data=messages
            )
        except KafkaError as e:
            error_msg = (
                f"Failed to consume messages from topic '{topic_name}': {str(e)}"
            )
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return Result(StatusCode.FAILURE, error_msg)

    def delete_topic(self, topic_name: str) -> Result:
        """
        Delete a Kafka topic.

        :param topic_name: The name of the topic to delete.
        :type topic_name: str
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            self.logger.info(f"Deleting Kafka topic '{topic_name}'...")

            self.admin_client.delete_topics([topic_name])
            return Result(
                StatusCode.SUCCESS, f"Topic '{topic_name}' deleted successfully."
            )
        except KafkaError as e:
            error_msg = f"Failed to delete topic '{topic_name}': {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return Result(StatusCode.FAILURE, error_msg)


"""
from assets.kafka_controller import KafkaController

def main():
    try:
        kafka = KafkaController(utils, bootstrap_servers=["localhost:9092"])

        # Example Kafka operations
        kafka.create_topic("test-topic")
        kafka.produce_message("test-topic", message_key=b"key1", message_value=b"Hello Kafka!")
        result = kafka.consume_messages("test-topic", group_id="test-group", max_messages=5)
        kafka.delete_topic("test-topic")
    except Exception as e:
        result = Result(StatusCode.FAILURE, f"Exception occurred: {str(e)}")
        app.utils.logger.error(traceback.format_exc())
    finally:
        final_emoji = "\U0001f600" if result.ret_cd == StatusCode.SUCCESS else "\U0001f61f"
        app.utils.logger.info(
            f'{time.strftime("%Y-%m-%d %H:%M:%S")} =>>>> {final_emoji} Final call at main with code: {result.ret_cd} and message: {result.ret_msg} <<<==='
        )
    return result.ret_cd

if __name__ == "__main__":
    exit(main())

"""
