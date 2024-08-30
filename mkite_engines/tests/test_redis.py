import os
import uuid
import fakeredis
import unittest as ut
from unittest.mock import patch

from mkite_core.models import JobInfo, JobResults, Status
from mkite_engines.redis import (
    RedisEngineSettings,
    RedisInfoSchema,
    RedisEngine,
    RedisProducer,
    RedisConsumer,
)


def get_info():
    return JobInfo(
        job={"uuid": str(uuid.uuid4())},
        recipe={"name": "test"},
        options={"param1": 1},
        inputs=[],
    )


def get_fake_redis(**kwargs):
    return fakeredis.FakeStrictRedis(**kwargs)


class TestRedisEngine(ut.TestCase):
    def setUp(self):
        self.settings = RedisEngineSettings()
        self.engine = RedisEngine.from_settings(self.settings)
        self.engine._r = get_fake_redis(**self.engine.redis_kwargs)

    def tearDown(self):
        self.engine.r.flushall()

    def test_format_queue_name(self):
        queue = "test"
        expected = self.engine.qprefix + queue
        returned = self.engine.format_queue_name(queue)
        self.assertEqual(expected, returned)

        queue = f"{self.engine.qprefix}test2"
        expected = queue
        returned = self.engine.format_queue_name(queue)
        self.assertEqual(expected, returned)

    def test_is_queue(self):
        queue = "test"
        self.assertFalse(self.engine.is_queue(queue))

        queue = f"{self.engine.qprefix}test2"
        self.assertTrue(self.engine.is_queue(queue))

    def test_list_queue(self):
        queue = f"{self.engine.qprefix}test"

        expected = []
        returned = self.engine.list_queue(queue)
        self.assertEqual(returned, expected)

        expected = ["a", "b", "c"]
        self.engine.r.rpush(queue, *expected)
        returned = self.engine.list_queue(queue)
        self.assertEqual(returned, expected)

    def test_list_queue_names(self):
        returned = self.engine.list_queue_names()
        self.assertEqual(returned, [])

        N_QUEUES = 4
        queues = [f"{self.engine.qprefix}{i}" for i in range(N_QUEUES)]
        for q in queues:
            self.engine.r.rpush(q, "item")

        expected = [f"{i}" for i in range(N_QUEUES)]
        returned = self.engine.list_queue_names()
        self.assertEqual(returned, expected)


class TestRedisProducer(ut.TestCase):
    def setUp(self):
        self.settings = RedisEngineSettings()
        self.prod = RedisProducer.from_settings(self.settings)
        self.prod._r = get_fake_redis(**self.prod.redis_kwargs)

    def tearDown(self):
        self.prod.r.flushall()

    def test_push(self):
        queue = f"{self.prod.qprefix}test"
        item = "test_item"

        self.prod.push(queue, item)
        returned = self.prod.r.lpop(queue).decode()

        self.assertEqual(returned, item)

    def test_push_info(self):
        info = get_info()
        key = info.uuid

        queue = f"{self.prod.qprefix}test"

        self.prod.push_info(queue, info)

        expected = [key]
        returned = [item.decode() for item in self.prod.r.lrange(queue, 0, -1)]
        self.assertEqual(expected, returned)

        status = self.prod.r.hget(key, "status").decode()
        self.assertEqual(status, Status.READY.value)

        msg = self.prod.r.hget(key, "msg")
        self.assertEqual(msg, info.encode())


class TestRedisConsumer(ut.TestCase):
    def setUp(self):
        self.settings = RedisEngineSettings()
        self.prod = RedisProducer.from_settings(self.settings)

        # connects both fakeredis caches
        self.prod._r = get_fake_redis(**self.prod.redis_kwargs)
        self.cons = RedisConsumer.from_settings(self.settings)
        self.cons._r = self.prod._r

    def tearDown(self):
        self.prod.r.flushall()
        self.cons.r.flushall()

    def test_get(self):
        queue = "test"

        info = get_info()
        key = info.uuid
        self.prod.push_info(queue, info)

        status = Status.READY.value
        returned_key, msg = self.cons.get(queue)

        self.assertEqual(key, returned_key)

        info_str = info.encode()
        self.assertEqual(msg, info_str)

        new_status = self.cons.r.hget(key, "status").decode()
        self.assertEqual(status, new_status)

    def test_get_info(self):
        queue = "test"

        info = get_info()
        key = info.uuid
        self.prod.push_info(queue, info)

        new_key, new_info = self.cons.get_info(queue)
        self.assertEqual(key, new_key)
        self.assertEqual(info, new_info)
