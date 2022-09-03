import dataclasses
import datetime
import json
import random
import time
import typing
import uuid


class Exporter(typing.Protocol):

    def export(self, msg: str):
        ...

    def export_info(self, msg: str):
        ...

class LogExporter:

    @staticmethod
    def export(msg: str):
        print(f"{datetime.datetime.now()} | MSG: {msg}")

    @staticmethod
    def export_info(msg: str):
        print(f"{datetime.datetime.now()} | MSG: {msg}")


@dataclasses.dataclass
class Experiment:
    name: str
    exporter: Exporter
    start_time: datetime.datetime = datetime.datetime.now()
    phase_state_one: int = 0
    phase_state_two: int = 0
    phase_state_three: int = 0
    current_phase: int = 1
    done: bool = False
    operator: str = "Anonymous"
    _change_chance: float = 0.0
    _last_timestamp: datetime.datetime = datetime.datetime.now()
    _last_value: float = 0.0
    _id = str = str(uuid.uuid4())


    def report_meta(self):
        msg = {"id": self._id,
               "name": self.name,
               "startTime": self.start_time.isoformat(),
               "operator": self.operator,
               "state": "started"
               }
        msg = json.dumps(msg)
        self.exporter.export_info(msg)

    def report_end(self):
        msg = {"id": self._id,
               "name": self.name,
               "startTime": self.start_time.isoformat(),
               "operator": self.operator,
               "state": "finished"
               }
        msg = json.dumps(msg)
        self.exporter.export(msg)

    def run(self):
        self.report_meta()
        last_phase = 3
        while self.current_phase <= last_phase:
            self.check_state_change()
            self.emit_value()
            time.sleep(10)
        #self.report_end()

    def emit_value(self):
        self._last_value = self._last_value + self.generate_noise(self.current_phase)
        self._last_timestamp = datetime.datetime.now()
        msg = self.prepare_json_message()
        self.exporter.export(msg)

    def  check_state_change(self):
        chance = random.uniform(0,100) / 100
        if chance < self._change_chance:
            self.change_phase()
        else:
            self._change_chance += 0.01

    def change_phase(self):
        if self.current_phase ==1:
                self.phase_state_one = 2
                self.phase_state_two = 1
        elif self.current_phase == 2:
                self.phase_state_two = 2
                self.phase_state_three = 1
        elif self.current_phase == 3:
                self.phase_state_three = 2
                self.done = 1

        self._last_value = 0
        self._change_chance = 0
        self.current_phase += 1

    def prepare_json_message(self) -> str:
        msg = {"EventId": str(uuid.uuid4()),
               "ExperimentId": self._id,
               "Phase": self.current_phase,
               "LastTime": self._last_timestamp.isoformat(),
               "LastValue": self._last_value}
        return json.dumps(msg)

    @staticmethod
    def stream_statement(name: str, topic: str, value_format: str = json, partitions: int = 4):
        return f"""CREATE STREAM {name} (
    EventId VARCHAR,
    ExperimentId VARCHAR,
    Phase INT,
    LastTime VARCHAR,
    LastValue DOUBLE
    )
    WITH (kafka_topic='{topic}', value_format='{value_format}', partitions={partitions});
    """

    @staticmethod
    def generate_noise(current_phase: int):
        return random.random() ** 2


if __name__ == '__main__':
    exporter = LogExporter()
    p = Experiment("test", exporter)
    p.run()