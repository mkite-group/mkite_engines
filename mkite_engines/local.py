import os
import time
import shutil
from typing import Sequence, List, Union
from tempfile import TemporaryDirectory

from pydantic import Field, DirectoryPath
from mkite_core.models import JobInfo, JobResults, Status
from mkite_engines.settings import EngineSettings

from .base import BaseEngine, BaseProducer, BaseConsumer


LOCAL_QUEUE_PREFIX = "queue-"


class LocalEngineSettings(EngineSettings):
    root_path: DirectoryPath = Field(
        os.path.expanduser("~/queue"),
        description="Where the jobs to be built will be placed",
    )
    move: bool = Field(
        False,
        description="If True, moves the paths when pushing",
    )
    delay: float = Field(
        2.0,
        description="Delay between considering the local folder as ready",
    )


class LocalEngine(BaseEngine):
    """Engine to implement a queue system using filesystem folders"""

    SETTINGS_CLS = LocalEngineSettings

    def __init__(
        self,
        root_path: os.PathLike,
        move: bool = False,
        return_abspath: bool = True,
        queue_prefix: str = LOCAL_QUEUE_PREFIX,
        delay: float = 2.0,
    ):
        self.root_path = os.path.abspath(root_path)
        self.mkdir(self.root_path)
        self.move = move
        self.return_abspath = return_abspath
        self.qprefix = queue_prefix
        self.delay = delay

    def __len__(self):
        return len(self.queues)

    def __repr__(self):
        n = len(self.queues)
        plural = "s" if n > 1 else ""
        return f"<{self.__class__.__name__} @ {self.root_path} ({n} queue{plural})>"

    def add_queue(self, name: str):
        if isinstance(name, Status):
            name = name.value

        if not self.is_queue(name):
            name = self.format_queue_name(name)

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
        if not os.path.isabs(path):
            path = self.abspath(path)

        elif not str(path).startswith(self.root_path):
            raise ValueError(
                "Not allowed to delete a file outside \
                of the path of the queue."
            )

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
        queue = self.format_queue_name(queue)
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

    def list_path(self, path: os.PathLike):
        return [entry for entry in os.listdir(path) if not entry.startswith(".")]

    def remove_path(self, item: os.PathLike):
        shutil.rmtree(item)

    def get_queue_path(self, queue: str) -> str:
        if isinstance(queue, Status):
            queue = queue.value

        if self.remove_queue_prefix(queue) not in self.queues:
            raise ValueError(f"Invalid queue {queue}")

        queue = self.format_queue_name(queue)
        return self.abspath(queue)

    def list_queue(self, queue: str) -> List[str]:
        """Get `n` items from the queue"""
        path = self.get_queue_path(queue)
        return self.list_path(path)

    def list_queue_names(self) -> List[str]:
        return [
            self.remove_queue_prefix(f)
            for f in self.list_path(self.root_path)
            if os.path.isdir(os.path.join(self.root_path, f)) and self.is_queue(f)
        ]

    @property
    def queues(self) -> List[str]:
        return self.list_queue_names()

    def set_status(self, key: str, status: str = Status.DOING.value):
        src = self.abspath(key)
        return self.move_path(status, src)


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
        status=Status.READY.value,
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

    def is_valid(self, path: os.PathLike):
        if os.path.basename(path).startswith("."):
            return False

        mtime = os.path.getmtime(path)
        now = time.time()

        return (now - mtime) > self.delay

    def get(self, queue: str) -> (str, str):
        """Get an item from the queue"""
        path = self.get_queue_path(queue)

        entries = self.list_path(path)

        if len(entries) == 0:
            return None, None

        item = entries[0]
        key = os.path.join(queue, item)

        if self.return_abspath:
            return key, os.path.join(path, item)

        return key, item

    def get_n(self, queue: str, n: int = 1000) -> (str, str):
        """Get `n` items from the queue"""
        path = self.get_queue_path(queue)

        i = 0
        for entry in os.scandir(path):
            if not self.is_valid(entry.path):
                continue

            if i >= n:
                break

            key = entry.name

            if self.return_abspath:
                item = os.path.join(path, key)
            else:
                item = key

            yield key, item

            i = i + 1

        return None, None

    def get_info(
        self, queue: str, info_cls=JobInfo
    ) -> (str, Union[JobInfo, JobResults]):
        """Get a JobInfo from the queue"""
        key, path = self.get(queue)
        if path is None:
            return None, None

        info = info_cls.from_json(path)

        return path, info
