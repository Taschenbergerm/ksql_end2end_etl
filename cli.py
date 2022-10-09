import os
import typing

from loguru import logger
import typer
import ksql
import uvicorn
from ksql.upload import FileUpload


from ksql_streaming.main import main
from ksql_streaming.api import api

app = typer.Typer()


@app.command("start worker")
def start():
    main()


@app.command("start api")
def restapi():
    uvicorn.run(api.app, port=int(os.getenv("API_PORT", 5000)), log_level="info")

@app.command()
def check_db(url: typing.Optional[str] = None):
    ...


@app.command()
def init_db(host: str = "http://0.0.0.0:8088"):
    pointer = FileUpload(host)
    try:
        pointer.upload('experiments.ksql')
    except ksql.errors.KSQLError as e:
        logger.warning(f"Encountered KSQL error - {e}")
    logger.success("Db initiated")


if __name__ == '__main__':
    app()