""" Module for publishing STAC messages """

__author__ = "Rhys Evans"
__date__ = "2022-08-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"


import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, UpdateByQuery

from stac_publisher.rabbit import RabbitProducer

log = logging.getLogger(__name__)


class Publisher:
    """
    Class to poll Elasticsearch for unaggregated assets or items and add
    the nessasary messages to the correct RabbitMQ queue
    """

    def __init__(self) -> None:

        config_file = os.environ.get("STAC_PUBLISHER_CONFIGURATION_FILE")
        if not config_file:
            config_file = os.path.join(
                Path(__file__).parent,
                ".stac_publisher.yml",
            )

        with open(config_file, encoding="utf-8") as reader:
            self.conf = yaml.safe_load(reader)

        self.es_conf = self.conf.get("ELASTICSEARCH")
        self.rabbit_conf = self.conf.get("RABBIT")
        self.log_conf = self.conf.get("LOGGING")

        logging.basicConfig(
            format="%(asctime)s @%(name)s [%(levelname)s]:    %(message)s",
            level=logging.getLevelName(self.log_conf.get("LEVEL")),
        )

        es_client = Elasticsearch(**self.es_conf.get("SESSION_KWARGS"))

        self.producer = RabbitProducer(self.rabbit_conf.get("SESSION_KWARGS"))

        self.search = Search(using=es_client, index=self.es_conf.get("INDEX")).filter(
            "term", status="new", size=10000
        )

        self.update = UpdateByQuery(using=es_client, index=self.es_conf.get("INDEX"))

    def get_messages(self, operator: str, cutoff: datetime) -> list:
        """
        The set of message that are older or younger than the cutoff time
        to be used to generate message list.

        :param cutoff: the time which older documents surtype can be generated
        :param operator: the comparison operator to use in the ES search
        :return: set of relevant messages
        """

        query = self.search.filter(
            "range", mod_time={operator: cutoff.strftime("%Y-%m-%dT%H:%M:%S")}
        )

        log.debug("Querying elasticsearch.")

        response = query.execute()

        hits = list(response.hits)

        log.info("Elasticsearch count: %s", response.hits.total)

        messages = {}
        for hit in hits:
            sur_id = hit[self.conf.get("ID_KEY")]

            log.debug(
                "Elasticsearch hit for %s : %s : %s",
                self.conf.get("ID_KEY"),
                sur_id,
                hit,
            )

            if sur_id not in messages:
                messages[sur_id] = {
                    "uri": sur_id,
                    "description_path": hit["description_path"],
                }

        log.info("Messages recieved: %s", messages)

        return messages

    def filter_messages(self, old_messages: dict, young_messages: dict) -> list:
        """
        The set of message that are older or younger than the cutoff time
        to be used to generate message list.

        :param cutoff: the time which older documents surtype can be generated
        :param operator: the comparison operator to use in the ES search
        :return: set of relevant messages
        """

        return [
            old_message
            for old_message in old_messages.values()
            if old_message["uri"] not in young_messages
        ]

    def publish_messages(self, messages: list) -> None:
        """
        The set of message that are older or younger than the cutoff time
        to be used to generate message list.

        :param cutoff: the time which older documents surtype can be generated
        :param operator: the comparison operator to use in the ES search
        :return: set of relevant messages
        """

        log.info("Publishing messages: %s", messages)

        with self.producer as producer:
            for message in messages:
                producer.publish(self.rabbit_conf.get("ROUTING_KEY"), message)

    def update_subs(self, messages: list) -> None:
        """
        Update the sub types 'status' facet.

        :param messages: list of messages include sur type ids
        :return: None
        """

        ids = [message["uri"] for message in messages]

        log.info("Updating ids: %s", ids)

        update_query = self.update.query(
            "terms", **{f"properties.{self.conf.get('ID_KEY')}": ids}
        ).script(source="ctx._source.status = 'queued'", lang="painless")

        update_query.execute()

    def run(self) -> None:
        """
        Generate and publish the surtype generation messages for the specified STAC type.

        :return: None
        """

        cutoff = datetime.now() - timedelta(minutes=self.conf.get("CUTOFF"))

        old_messages = self.get_messages("lte", cutoff)

        young_messages = self.get_messages("gt", cutoff)

        messages = self.filter_messages(old_messages, young_messages)

        self.publish_messages(messages)

        self.update_subs(messages)


if __name__ == "__main__":
    Publisher().run()
