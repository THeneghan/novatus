"""Novatus's cli"""

import logging
from typing import Annotated, Optional

import typer
from novatus_utils.db import create_postgres_container, destroy_postgres_container
from novatus_utils.logging_utils import setup_logging

setup_logging()
logger = logging.getLogger(__name__)
app = typer.Typer()


@app.callback()
def callback():
    pass


@app.command()
def create_generic_local_db(container_name: Annotated[Optional[str], typer.Argument()] = "novatus_local_db"):
    logger.info(f"Creating local postgres db in container name {container_name}")
    create_postgres_container(container_name=container_name, POSTGRES_PASSWORD="password")
    logger.info(f"local postgres db created in {container_name}")


@app.command()
def destroy_local_db(container_name: str):
    logger.info(f"Destroying local postgres db in container name {container_name}")
    destroy_postgres_container(container_name=container_name)
    logger.info(f"Destroyed local postgres db in container name {container_name}")
