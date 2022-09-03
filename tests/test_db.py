import pytest

from ksql_streaming.api import db



def test_query():
    backend = db.Backend(host="http://0.0.0.0:8088")
    res = backend.query("SELECT * FROM EXPERIMENTS")
    assert res.as_records()