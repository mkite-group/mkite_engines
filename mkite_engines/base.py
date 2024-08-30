import os
from enum import Enum
from typing import List, Dict, Union
from abc import ABC, abstractmethod
from mkite_core.external import load_config

from mkite_core.models import JobInfo, JobResults
from .settings import EngineSettings


class EngineError(Exception):
    pass


class EngineRoles(Enum):
    producer = "producer"
    consumer = "consumer"


class BaseEngine(ABC):
    """Manages the flow of information to/from mkite/mkwind and their processes.
    As mkite and mkwind are not coupled directly, an intermediate engine has to
    act as the middleware for their communication.
    """

    SETTINGS_CLS = EngineSettings

    def __init__(
        self,
        *args,
        queue_prefix: str = "queue:",
        **kwargs
    ):
        self.qprefix = queue_prefix

    @classmethod
    def from_settings(cls, settings: EngineSettings) -> "BaseEngine":
        """Creates the engine from a pydantic EngineSettings"""
        return cls(**settings.model_dump())

    @classmethod
    def from_env(cls) -> "BaseEngine":
        """Creates the engine from environmental variables"""
        settings = cls.SETTINGS_CLS()
        return cls.from_settings(settings)

    @classmethod
    def from_file(cls, filename: os.PathLike) -> "BaseEngine":
        """Creates the engine from a configuration file"""
        settings = cls.SETTINGS_CLS.from_file(filename)
        return cls.from_settings(settings)

    def format_queue_name(self, queue: str):
        if self.is_queue(queue):
            return queue

        return self.qprefix + queue

    def remove_queue_prefix(self, queue: str):
        return queue.replace(self.qprefix, "")

    def is_queue(self, key: str) -> bool:
        return key.startswith(self.qprefix)

    @abstractmethod
    def list_queue(self, queue: str) -> List[str]:
        """Fully lists a given queue"""

    def list_all_queues(self) -> List[str]:
        """Fully lists all queues"""
        items = []
        for queue in self.list_queue_names():
            items += self.list_queue(queue)

        return items

    @abstractmethod
    def list_queue_names(self) -> List[str]:
        """Lists all names of all queues in the engine"""

    def is_info(self, item) -> bool:
        """Returns True if `item` is an instance of JobInfo or JobResults"""
        return isinstance(item, (JobInfo, JobResults))

    @abstractmethod
    def add_queue(self, name: str):
        """Creates a new queue in the engine"""

    @abstractmethod
    def delete(self, key: str):
        """Deletes an item indexed by `key` from the engine"""


class BaseProducer(BaseEngine):
    @abstractmethod
    def push(self, queue: str, item: str):
        """Adds the given item to the queue"""

    def push_info(self, queue: str, info: JobInfo):
        return self.push(queue, info.encode())


class BaseConsumer(BaseEngine):
    @abstractmethod
    def get(self, queue: str) -> (str, str):
        """Get an item from the queue"""

    def pop(self, queue: str) -> (str, str):
        key, item = self.get(queue)

        if key is not None:
            self.delete(key)

        return key, item

    def get_n(self, queue: str, n: int = 1000) -> (str, str):
        """Get n items from the queue"""
        i = 0
        while i < n:
            key, item = self.get(queue)
            if not item:
                break

            yield key, item
            i = i + 1

        return None, None

    def get_info(self, queue: str, info_cls=JobInfo) -> (str, Union[JobInfo, JobResults]):
        """Get a JobInfo from the queue"""
        key, item = self.get(queue)

        if item is None:
            return None, None

        return key, info_cls.decode(item)
