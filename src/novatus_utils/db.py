"""Postgres db related utils"""

import time

import docker
from novatus_utils.sockets_ports import port_in_use
from sqlalchemy import Engine, create_engine


def create_postgres_container(container_name: str, host_port: int = 5432, host: str = "127.0.0.1", **docker_env_vars):
    """Creates a postgres db in a docker container"""
    client = docker.from_env()
    container_names = [container.name for container in client.containers.list()]
    if container_name not in container_names and not port_in_use(host_port, host):
        client.containers.run(
            "postgres",
            environment={**docker_env_vars},
            detach=True,
            name=container_name,
            ports={"5432/tcp": (host, host_port)},
        )
        time.sleep(1)  # Needed as sometimes a delay to build


def destroy_postgres_container(container_name: str):
    """Stops and removes/deletes a container"""
    client = docker.from_env()
    db_container = [container for container in client.containers.list() if container.name == container_name][0]
    db_container.stop()
    db_container.remove()


def create_postgres_sqlalchemy_engine(
    user: str, password: str, host: str = "127.0.0.1", db_name: str = "postgres", port: int = 5432, echo: bool = False
) -> Engine:
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}", echo=echo)
