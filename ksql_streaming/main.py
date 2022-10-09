import dataclasses
import logging
import random
import string
import multiprocessing as mp
import time
import typing

import kafka
from loguru import logger

from ksql_streaming.backend import producer


def random_name():
    length = random.randint(5,20)
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


@dataclasses.dataclass
class KafkaExporter():

    bootstrap_servers: typing.Union[typing.List[str], str] = "0.0.0.0:9093"
    prefix: str = "experiment"
    producer: typing.Optional[kafka.KafkaProducer] = None

    def __post_init__(self):

        if isinstance(self.bootstrap_servers, str):
            self.bootstrap_servers = [self.bootstrap_servers]

        self.producer = kafka.KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                            value_serializer=lambda v: v.encode('utf-8'))


    def export(self, msg: str):
        topic = f"{self.prefix}-events"
        logger.debug(f"Exporter | WAL | {topic} | {msg}")
        future = self.producer.send(f"{topic}", msg)
        future.get(10)
        logger.success(f"Exporter | SUC | {topic} |{msg}")

    def export_info(self, msg):
        topic = f"{self.prefix}-meta"
        logger.debug(f"Exporter | WAL | {topic} | {msg}")
        future = self.producer.send(f"{topic}", msg)
        future.get(10)
        logger.success(f"Exporter | SUC | {topic} |{msg}")


def agent():
    exporter = KafkaExporter()
    while True:
        experiment = producer.Experiment(
            name=random_name(),
            exporter=exporter
        )
        experiment.run()


def main():

    processes = [mp.Process(target=agent) for _ in range(3)]

    for p in processes:
        p.start()

    try:
        while True:
            logger.info("Master | running")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.warning("Master | Graceful shutdown initiated")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
    finally:
        logging.info("Good Bye")
