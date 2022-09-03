import logging
import random
import string
import multiprocessing as mp
import time

import kafka
from loguru import logger

from ksql_streaming.backend import producer

import faust



def random_name():
    length = random.randint(5,20)
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])

class KafkaExporter():

    def __init__(self, prefix: str = "experiment"):
        self.producer = kafka.KafkaProducer(bootstrap_servers=["0.0.0.0:9093"],
                                            value_serializer=lambda v: v.encode('utf-8'))
        self.prefix = prefix

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

if __name__ == '__main__':
    main()