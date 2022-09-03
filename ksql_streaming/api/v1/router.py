import  fastapi
import ksql

v1 = fastapi.APIRouter(prefix="/v1")
client = ksql.KSQLAPI('http://ksql-server:8088')



@v1.get("/data")
def query_data():
    "List the potential data sources"
    client


@v1.get("/data/{source}")
def query_data(source: str ):
    "Get the data from a given datasource within the system"
    ...


@v1.get("/experiments")
def get_experiments():
    """Endpoint to list all experiments"""
    ...

@v1.get("/experiments/{id}")
def get_experiment_by_id(id: str):
    """ Endpoint to acquire the data regarding one Experiment by ID"""
    ...