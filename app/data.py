from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any


class ServerRole(StrEnum):
    MASTER = auto()  # determine if the data is contaminated
    SLAVE = auto()  # determine if the onboarding crop is necessary.


@dataclass
class Error:
    message: str


@dataclass
class ServerInfo:
    role: ServerRole
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0


@dataclass
class ReplicaConf:
    host: str
    port: int


@dataclass
class ServerConfiguration:
    info: ServerInfo
    replica_conf: ReplicaConf | None = None


@dataclass
class RedisData:
    value: Any
    expire_at: float | None
