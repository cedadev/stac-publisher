""" Module for connecting to RabbitMQ Queue """

from __future__ import annotations

__author__ = "Rhys Evans"
__date__ = "2022-08-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"

import json
import logging

import pika
from pika.adapters.blocking_connection import BlockingChannel

log = logging.getLogger(__name__)


class RabbitConfiguration:
    """
    Class to store RabbitMQ config
    """

    def __init__(self, config: dict) -> None:

        credentials = pika.PlainCredentials(config.get("USER"), config.get("PASSWORD"))

        self._connection_parameters = pika.ConnectionParameters(
            host=config.get("HOST"),
            credentials=credentials,
            virtual_host=config.get("VHOST"),
            heartbeat=config.get("HEARTBEAT"),
        )

        self._exchange = config.get("EXCHANGE")

    @property
    def exchange(self) -> dict:
        """
        Exchange for Rabbit
        """
        return self._exchange

    @property
    def connection_parameters(self) -> dict:
        """
        Connection parameters for Rabbit
        """
        return self._connection_parameters


class RabbitConnection:
    """
    Class to maintain RabbitMQ Connection
    """

    @property
    def channel(self) -> BlockingChannel | None:
        """
        Channel of Rabbit Connection
        """
        return self._channel

    @property
    def exchange(self) -> dict:
        """
        Exchange of Rabbit Connection
        """
        return self._configuration.exchange

    def __init__(self, config: dict) -> None:

        self._connection = None
        self._channel = None

        self._configuration = RabbitConfiguration(config)

    def __enter__(self) -> RabbitConnection:
        self.connect()
        return self

    def __exit__(self, *args, **kwargs) -> None:

        self.close()

    def connect(self) -> None:
        """
        Connect to the RabbitMQ server

        :return: None
        """

        log.debug("Initialising new RabbitMQ connection.")
        self._connection = pika.BlockingConnection(
            self._configuration.connection_parameters
        )

        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self.exchange.get("NAME"),
            exchange_type=self.exchange.get("TYPE", "topic"),
            **self.exchange.get("KWARGS", {}),
        )

    def close(self) -> None:
        """
        Close connection to the RabbitMQ server

        :return: None
        """

        log.debug("Closing RabbitMQ connection.")
        self._connection.close()
        self._channel = None


class RabbitProducer:
    """
    Class to add messages to Rabbit queue
    """

    def __init__(self, config: dict) -> None:

        self._connection = RabbitConnection(config)

    def __enter__(self) -> RabbitProducer:

        self._connection.connect()
        return self

    def __exit__(self, *args, **kwargs) -> None:

        self._connection.close()

    def publish(self, routing_key: str, body: dict) -> None:
        """
        Publish message to RabbitMQ queue

        :param routing_key: Routing key for queue
        :param body: Message

        :return: None
        """

        log.debug(
            "Publishing message to exchange %s, with routing key %s",
            self._connection.exchange.get("NAME"),
            routing_key,
        )
        self._connection.channel.basic_publish(
            exchange=self._connection.exchange.get("NAME"),
            routing_key=routing_key,
            body=json.dumps(body),
        )
