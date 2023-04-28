import os
import shutil
from typing import Sequence, List, Union
from tempfile import TemporaryDirectory

from pydantic import Field, DirectoryPath
from mkite_core.models import JobInfo, JobResults
from mkite_engines.settings import EngineSettings

from .status import Status
from .base import BaseEngine, BaseProducer, BaseConsumer


class LocalEngineSettings(EngineSettings):
    root_path: DirectoryPath = Field(
        os.path.expanduser("~/queue"),
        description="Where the jobs to be built will be placed",
    )


class LocalEngine(BaseEngine):
    """Engine to implement a queue system using filesystem folders"""

    SETTINGS_CLS = LocalEngineSettings

    def __init__(
        self,
        root_path: os.PathLike,
        move: bool = False,
        return_abspath: bool = True,
    ):
        self.root_path = os.path.abspath(root_path)
        self.mkdir(self.root_path)
        self.move = move
        self.return_abspath = return_abspath

    def __len__(self):
        return len(self.queues)

    def __repr__(self):
        n = len(self.queues)
        plural = "s" if n > 1 else ""
        return f"<{self.__class__.__name__} @ {self.root_path} ({n} queue{plural})>"

    def add_queue(self, name: str):
        if isinstance(name, Status):
            name = name.value

        if name not in self.queues:
            self.setup_path(name)

    def abspath(self, folder: os.PathLike):
        return os.path.join(self.root_path, folder)

    def setup_path(self, name: str):
        path = self.abspath(name)
        self.mkdir(path)
        return path

    def mkdir(self, path: os.PathLike):
        if os.path.exists(path):
            return

        os.mkdir(path)

    def delete(self, path: os.PathLike):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Path {path} does not exist")

        if not os.path.isdir(path):
            os.remove(path)
            return

        shutil.rmtree(path)

    def is_path(self, path: str):
        return os.path.exists(path)

    def item_path(self, queue: str, item: os.PathLike):
        name = os.path.basename(item)
        dst = self.abspath(queue)
        return os.path.join(dst, name)

    def copy_path(self, queue: str, item: os.PathLike):
        dst = self.item_path(queue, item)

        if os.path.isdir(item):
            shutil.copytree(item, dst)
        else:
            shutil.copy(item, dst)

        return dst

    def move_path(self, queue: str, item: os.PathLike):
        dst = self.item_path(queue, item)
        shutil.move(item, dst)

        return dst

    def remove_path(self, item: os.PathLike):
        shutil.rmtree(item)

    def get_queue_path(self, queue: str) -> str:
        if isinstance(queue, Status):
            queue = queue.value

        if queue not in self.queues:
            raise ValueError(f"Invalid queue {queue}")

        return self.abspath(queue)

    def list_queue(self, queue: str) -> List[str]:
        """Get `n` items from the queue"""
        path = self.get_queue_path(queue)
        return os.listdir(path)

    def list_queue_names(self) -> List[str]:
        return [
            f for f in os.listdir(self.root_path)
            if os.path.isdir(os.path.join(self.root_path, f))
        ]

    @property
    def queues(self) -> List[str]:
        return self.list_queue_names()


class LocalProducer(LocalEngine, BaseProducer):
    """Producer that submits a folder to a new directory"""

    def push(
        self,
        queue: str,
        item: os.PathLike,
        add_queue: bool = True,
    ):
        if queue not in self.queues and add_queue:
            self.add_queue(queue)

        queue = self.get_queue_path(queue)

        if not self.is_path(item):
            raise ValueError(f"Cannot submit {item}: invalid type")

        if self.move:
            dst = self.move_path(queue, item)
        else:
            dst = self.copy_path(queue, item)

        return dst

    def push_info(
        self,
        queue: str,
        info: Union[JobInfo, JobResults],
        name: str = None,
    ):
        if name is None and self.is_info(info):
            name = info.uuid

        if not name.endswith(".json"):
            name = name + ".json"

        with TemporaryDirectory() as tmp:
            path = os.path.join(tmp, name)
            info.to_json(path)
            dst = self.push(queue, path)

        return dst


class LocalConsumer(LocalEngine, BaseConsumer):
    """Consumer that uses folders as queue"""

    def get(self, queue: str):
        """Get an item from the queue"""
        path = self.get_queue_path(queue)

        entries = [entry for entry in os.listdir(path) if not entry.startswith(".")]

        if len(entries) == 0:
            return None

        item = entries[0]

        if self.return_abspath:
            return os.path.join(path, item)

        return item

    def get_n(self, queue: str, n: int = 1000):
        """Get `n` items from the queue"""
        path = self.get_queue_path(queue)

        i = 0
        for entry in os.scandir(path):
            if entry.name.startswith("."):
                continue

            if i >= n:
                break

            item = entry.name

            if self.return_abspath:
                item = os.path.join(path, item)

            yield item

            i = i + 1

    def get_info(self, queue: str, info_cls=JobInfo):
        """Get a JobInfo from the queue"""
        msg = self.get(queue)
        if msg is None:
            return None

        info = info_cls.from_json(msg)
        self.delete(msg)
        return info
