import os

import fastapi

from ksql_streaming.api import db




app = fastapi.FastAPI()
states = fastapi.APIRouter(prefix="/states")
experiments = fastapi.APIRouter(prefix="/experiments")


@app.on_event("startup")
def configure_app():
    app.state.ksqldb_host = os.getenv("KSQLDB_HOST", "http://0.0.0.0:8088")

@app.get("/query")
def query(resp: fastapi.Response):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.query("SELECT * FROM EXPERIMENTS")
    return res.as_records()


@states.get("/phase")
def get_phase_states(resp: fastapi.Response):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_phase_states()
    return res.as_records()


@states.get("/phase/{state_id}")
def get_phase_state_by_id(resp: fastapi.Response, state_id: str):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_phase_state_by_id(state_id)
    return res.as_records()


@states.get("/")
def get_states(resp: fastapi.Response):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_states()
    return res.as_records()


@states.get("/{state_id}")
def get_state_by_id(resp: fastapi.Response, state_id: str):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_state_by_id(state_id)
    return res.as_records()


@experiments.get("/")
def get_experiment_states(resp: fastapi.Response):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_experiment_states()
    return res.as_records()


@experiments.get("/{state_id}")
def get_experiment_state_by_id(resp: fastapi.Response, state_id: str):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_experiment_state_by_id(state_id)
    return res.as_records()

@experiments.get("/")
def get_experiments(resp: fastapi.Response):
    backend = db.Backend(host=app.state.ksqldb_host)
    res = backend.get_experiments()
    return res.as_records()


app.include_router(states)
app.include_router(experiments)