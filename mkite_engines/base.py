import os
from enum import Enum
from typing import List, Dict
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

    @classmethod
    def from_settings(cls, settings: EngineSettings) -> "BaseEngine":
        """Creates the engine from a pydantic EngineSettings"""
        return cls(**settings.dict())

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

class BaseProducer(BaseEngine):
    @abstractmethod
    def push(self, queue: str, item: str):
        """Adds the given item to the queue"""

    def push_info(self, queue: str, info: JobInfo):
        return self.push(queue, info.encode())


class BaseConsumer(BaseEngine):
    @abstractmethod
    def get(self, queue: str):
        """Get an item from the queue"""

    def get_n(self, queue: str, n: int = 1000):
        """Get n items from the queue"""
        i = 0
        while (i < n):
            item = self.get(queue)
            if not item:
                break
            
            yield item
            i = i + 1

    def get_info(self, queue: str, info_cls=JobInfo):
        """Get a JobInfo from the queue"""
        msg = self.get(queue)
        return info_cls.decode(msg)
