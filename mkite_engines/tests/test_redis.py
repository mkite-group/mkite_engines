import os
import uuid
import fakeredis
import unittest as ut
from unittest.mock import patch

from mkite_core.models import JobInfo, JobResults
from mkite_engines.redis import (
    RedisEngineSettings,
    RedisInfoSchema,
    RedisEngine,
    RedisProducer,
    RedisConsumer,
)
from mkite_engines.status import Status


def get_info():
    return JobInfo(
        job={"uuid": str(uuid.uuid4())},
        recipe={"name": "test"},
        options={"param1": 1},
        inputs=[],
    )


@patch("redis.Redis", fakeredis.FakeStrictRedis)
class TestRedisEngine(ut.TestCase):
    def setUp(self):
        self.settings = RedisEngineSettings()

    def get_engine(self):
        return RedisEngine.from_settings(self.settings)

    def test_init(self):
        engine = self.get_engine()

    def test_format_queue_name(self):
        engine = self.get_engine()

        queue = "test"
        expected = engine.qprefix + queue
        returned = engine.format_queue_name(queue)
        self.assertEqual(expected, returned)

        queue = f"{engine.qprefix}test2"
        expected = queue
        returned = engine.format_queue_name(queue)
        self.assertEqual(expected, returned)

    def test_is_queue(self):
        engine = self.get_engine()

        queue = "test"
        self.assertFalse(engine.is_queue(queue))

        queue = f"{engine.qprefix}test2"
        self.assertTrue(engine.is_queue(queue))

    def test_list_queue(self):
        engine = self.get_engine()
        queue = f"{engine.qprefix}test"

        expected = []
        returned = engine.list_queue(queue)
        self.assertEqual(returned, expected)

        expected = ["a", "b", "c"]
        engine.r.rpush(queue, *expected)
        returned = engine.list_queue(queue)
        self.assertEqual(returned, expected)

    def test_list_queue_names(self):
        engine = self.get_engine()

        returned = engine.list_queue_names()
        self.assertEqual(returned, [])

        N_QUEUES = 4
        queues = [f"{engine.qprefix}{i}" for i in range(N_QUEUES)]
        for q in queues:
            engine.r.rpush(q, "item")

        expected = [f"{i}" for i in range(N_QUEUES)]
        returned = engine.list_queue_names()
        self.assertEqual(returned, expected)


@patch("redis.Redis", fakeredis.FakeStrictRedis)
class TestRedisProducer(ut.TestCase):
    def setUp(self):
        self.settings = RedisEngineSettings()

    def get_producer(self):
        return RedisProducer.from_settings(self.settings)

    def test_push(self):
        pub = self.get_producer()
        queue = f"{pub.qprefix}test"
        item = "test_item"

        pub.push(queue, item)
        returned = pub.r.lpop(queue).decode()

        self.assertEqual(returned, item)

    def test_push_info(self):
        pub = self.get_producer()
        info = get_info()
        key = info.uuid

        queue = f"{pub.qprefix}test"

        pub.push_info(queue, info)

        expected = [key]
        returned = [item.decode() for item in pub.r.lrange(queue, 0, -1)]
        self.assertEqual(expected, returned)

        status = pub.r.hget(key, "status").decode()
        self.assertEqual(status, Status.READY.value)

        msg = pub.r.hget(key, "msg")
        self.assertEqual(msg, info.encode())


@patch("redis.Redis", fakeredis.FakeStrictRedis)
class TestRedisConsumer(ut.TestCase):
    def setUp(self):
        self.settings = RedisEngineSettings()

    def get_consumer(self):
        return RedisConsumer.from_settings(self.settings)

    def get_producer(self):
        return RedisProducer.from_settings(self.settings)

    def test_get(self):
        cons = self.get_consumer()
        pub = self.get_producer()
        cons.r = pub.r
        queue = f"{cons.qprefix}test"

        info = get_info()
        key = info.uuid
        pub.push_info(queue, info)

        status = Status.DOING.value
        msg = cons.get(queue, status=status)

        info_str = info.encode()
        self.assertEqual(msg, info_str)

        new_status = cons.r.hget(key, "status").decode()
        self.assertEqual(status, new_status)

    def test_get_info(self):
        cons = self.get_consumer()
        pub = self.get_producer()
        cons.r = pub.r
        queue = f"{cons.qprefix}test"

        info = get_info()
        key = info.uuid
        pub.push_info(queue, info)

        new_info = cons.get_info(queue)
        self.assertEqual(info, new_info)
