import pytest

from ksql_streaming.api import db

@pytest.fixture(scope="session")
def backend():
    return db.Backend(host="http://0.0.0.0:8088")

def test_query(backend):
    backend = db.Backend(host="http://0.0.0.0:8088")
    res = backend.query("SELECT * FROM EXPERIMENTS")
    assert res.as_records()

def test_get_phase_states(backend):
    res = backend.get_phase_states()
    assert res.as_records()

def test_get_phase_state_by_id(backend):
    want = backend.get_phase_states().as_records()[0]
    got = (backend.get_phase_state_by_id(want["ID"]),
           backend.get_phase_state_by_name(want["NAME"])
           )
    assert got[0] == want == got[1]

def test_get_states(backend):
    res = backend.get_states()
    assert res.as_records()

def test_get_state_by_id(backend):
    want = backend.get_states().as_records()[0]
    got = backend.get_state_by_id(want["ID"])
    assert want == got

def test_get_experiment_states(backend):
    res = backend.get_experiment_states()
    assert res.as_records()

def test_get_experiment_state_by_id(backend):
    want = backend.get_experiment_states().as_records()[0]
    got = backend.get_experiment_state_by_id(want["ID"])
    assert want == got

def test_get_experiments(backend):
    want = backend.get_experiments()
    assert want.as_records()