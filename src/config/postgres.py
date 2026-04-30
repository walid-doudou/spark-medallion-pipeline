import os

__all__ = ["JDBC_URL", "JDBC_PROPS"]

_HOST = os.getenv("POSTGRES_GOLD_HOST", "localhost")
_PORT = os.getenv("POSTGRES_GOLD_PORT", "5432")
_DB = os.getenv("POSTGRES_GOLD_DB", "gold")
_USER = os.getenv("POSTGRES_GOLD_USER", "gold")
_PASSWORD = os.getenv("POSTGRES_GOLD_PASSWORD", "gold")

JDBC_URL = f"jdbc:postgresql://{_HOST}:{_PORT}/{_DB}"
JDBC_PROPS = {"user": _USER, "password": _PASSWORD, "driver": "org.postgresql.Driver"}
