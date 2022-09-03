import dataclasses
import datetime
import typing

import pytest

import ksql_streaming.backend.producer


@dataclasses.dataclass
class MockExporter:
    got: list[typing.Any] = dataclasses.field(default_factory=lambda: [])

    def export(self, msg: str):
        self.got.append(msg)

    def export_info(self, msg: str):
        self.got.append(msg)


@pytest.fixture(scope="function")
def mock_exporter():
    return MockExporter()


def test_experiment_report_meta(mock_exporter):
    given = {"name": "test-experiment",
             "exporter":mock_exporter,
             "start_time": datetime.datetime(2022,1,2,3,4,5,678),
             "operator": "test-operator",
             "_id": "test_id"}
    want = given
    experimet = ksql_streaming.backend.producer.Experiment(**given)
    got = experimet.exporter.got

    for key, value in given.items():
        assert got[key] == want[key]