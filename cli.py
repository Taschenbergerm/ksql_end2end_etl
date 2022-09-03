import typing

from loguru import logger
import typer
import ksql
from ksql.upload import FileUpload


app = typer.Typer()


@app.command()
def start():
    ...


@app.command()
def check_db(url: typing.Optional[str] = None):
    ...


@app.command()
def init_db(host: str = "http://0.0.0.0:8088"):
    pointer = FileUpload(host)
    pointer.upload('experiments.ksql')
    logger.success("Db initiated")


if __name__ == '__main__':
    app()