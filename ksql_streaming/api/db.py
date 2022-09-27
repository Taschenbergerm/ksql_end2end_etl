import typing
import dataclasses

import logger
import requests


@dataclasses.dataclass
class KSQLAPI:
    host: str = "localhost"
    port: int = 8088
    max_retries: int = 3

    url: typing.Optional[str] = ""
    _header: dict = dataclasses.field(default_factory=lambda: {"Accept": "application/vnd.ksql.v1+json"})
    info: dict  = None

    def __post_init__(self):
        self.info = self.get_info()

        if not self.url:
           self.generate_url()

    def generate_url(self):
        self.url = f"http://{self.host}:{self.port}"

    def query(self, query: str, *args, **kwargs):
        query = self._trim_query(query)
        endpoint = f"{self.url}/query"
        body = {"ksql": query, "streamProperties": ""}
        resp = requests.post(endpoint, json=body, headers=self._header)
        return KsqlResponse(response=resp)

    def get_info(self):
        resp = requests.get(self.url+ "/info")
        return resp.json()

    def ksql(self, query: str):
        endpoint = f"{self.url}/ksql"
        resp = requests.post(endpoint, data={"ksql": query})
        return resp.json()

    @staticmethod
    def _trim_query(query: str):
        stripped = query.strip()
        if stripped[-1] != ";":
            stripped += ";"
        return stripped


@dataclasses.dataclass
class KsqlResponse:
    response: requests.Response
    columns: list = None
    values: list = None
    data: dict = None
    ok: bool = False
    status_code: int = 0

    def __post_init__(self):
        self.ok = self.response.ok
        self.status_code = self.response.status_code
        if self.ok:
            self.data = self.response.json()
            self.values = [row["row"]["columns"] for row in self.response.json()[1:]]
            self.columns = self.clean_columns(self.data[0]["header"]["schema"])
        else:
            self.data = {}
            self.values = []
            self.columns = []

    @staticmethod
    def clean_columns(schema: str):
        res = []
        for component in schema.split(","):
            column = component.strip().split(" ")[0]
            res.append(column.strip("`"))
        return res

    def as_df(self):
        try:
            import pandas as pd
        except ImportError:
            logger.error("""To use KSQLResponse.as_df please install pandas 
            try:
                $ python -m pip install pandas
            """)
        return pd.DataFrame(self.data, columns=self.columns)

    def as_records(self):
        res = []
        for row in self.values:
            record = {}
            for key, val in zip(self.columns, row):
                record[key]= val
            res.append(record)
        return res

    def __iter__(self):
        return iter(self.as_records())

@dataclasses.dataclass
class Backend:
    host: str
    _client: KSQLAPI = None

    def __post_init__(self):
        self._client = KSQLAPI(url=self.host)

    def get_sources(self):
        tables = [ entry for entry in self._client.ksql("SHOW TABLES;")]
        streams = [ entry  for entry in self._client.ksql("SHOW STREAMS;")]
        return {"tables": tables, "streams": streams}

    def get_phase_states(self):
        query = "SELECT * FROM PHASESTATES"
        return self.query(query)

    def get_phase_state_by_id(self, id: str):
        query = f"SELECT * FROM PHASESTATES WHERE id= '{id}'"
        return self.query(query)

    def get_phase_state_by_name(self, name: str):
        query = f"SELECT * FROM PHASESTATES WHERE name = '{name}'"
        return self.query(query)

    def get_states(self):
        query = "SELECT * FROM STATES"
        return self.query(query)

    def get_state_by_id(self, id: str):
        query = f"SELECT * FROM STATE WHERE id = '{id}'"
        return self.query(query)

    def get_experiment_states(self):
        query = "SELECT * FROM EXPERIMENTSTATES "
        return self.query(query)

    def get_experiment_state_by_id(self):
        query = f"SELECT * FROM EXPERIMENTSTATES WHERE id = '{id}'"
        return self.query(query)

    def get_experiments(self):
        query = "SELECT * FROM EXPERIMENTS"
        return self.query(query)

    def query(self, query: str, *args, **kwargs):
        return self._client.query(query=query, *args, **kwargs)

