import json
import redis

from typing import List, Union
from pydantic import Field, DirectoryPath, BaseModel
from mkite_engines.settings import EngineSettings
from mkite_core.models import JobInfo, JobResults

from .status import Status
from .base import BaseEngine, BaseProducer, BaseConsumer


class RedisEngineSettings(EngineSettings):
    host: str = Field(
        "localhost",
        description="hostname or IP address of the Redis server",
    )
    port: int = Field(
        6379,
        description="port of the Redis server",
    )
    password: str = Field(
        None,
        description="port of the Redis server",
    )
    ssl: bool = Field(False, description="whether to use ssl when connecting")
    ssl_cert_reqs: str = Field(
        "required",
        description="requirements of ssl certificates",
    )

    class Config:
        env_prefix = "REDIS_"
        case_sensitive = False


class RedisInfoSchema(BaseModel):
    msg: str
    status: str

    def items(self):
        return self.dict().items()


class RedisEngine(BaseEngine):
    SETTINGS_CLS = RedisEngineSettings

    def __init__(
        self,
        host: str,
        port: int,
        password: str = None,
        queue_prefix: str = "queue:",
        **kwargs,
    ):
        self.r = redis.Redis(host=host, port=port, password=password, **kwargs)
        self.qprefix = queue_prefix

    def list_queue(self, queue: str) -> List[str]:
        queue = self.format_queue_name(queue)
        items = self.r.lrange(queue, 0, -1)
        return [i.decode() for i in items]

    def list_queue_names(self) -> List[str]:
        queues = self.r.keys(self.qprefix + "*")
        queues = [k.decode() for k in queues]
        queues = [self.remove_queue_prefix(k) for k in queues]
        return queues

    def add_queue(self, name: str):
        """Empty queues do not have to be created in Redis.
        This method exists for compatibility with other engines.
        """
        pass

    def set_status(self, key: str, status: str = Status.DOING.value):
        self.r.hset(key, "status", status)

    def delete(self, key: str):
        self.r.delete(key)


class RedisProducer(RedisEngine, BaseProducer):
    def push(self, queue: str, item: str, left: bool = True):
        queue = self.format_queue_name(queue)
        if left:
            return self.r.lpush(queue, item)
        return self.r.rpush(queue, item)

    def push_info(
        self,
        queue: str,
        info: Union[JobInfo, JobResults],
        status=Status.READY.value,
    ):
        msg = info.encode()
        key = str(info.uuid)

        data = RedisInfoSchema(
            msg=msg,
            status=status,
        )

        self.r.hset(key, mapping=data)
        return self.push(queue, key)


class RedisConsumer(RedisEngine, BaseConsumer):
    def get(self, queue: str) -> (str, str):
        if queue not in self.list_queue_names():
            return None, None

        queue = self.format_queue_name(queue)
        key = self.r.lpop(queue).decode()
        msg = self.r.hget(key, "msg")

        return key, msg
