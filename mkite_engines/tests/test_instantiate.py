import os
import unittest as ut
from unittest.mock import patch
from pkg_resources import resource_filename

from mkite_core.models import Status
from mkite_engines.base import EngineError, EngineRoles
from mkite_engines.settings import EngineSettings
from mkite_engines.local import LocalProducer, LocalConsumer

from mkite_engines.instantiate import (
    get_engine_class,
    instantiate_from_dict,
    instantiate_from_path,
)

SETTINGS_PATH = resource_filename("mkite_engines.tests.configs", "local.yaml")


class TestInstantiate(ut.TestCase):
    def setUp(self):
        self.module = "mkite_engines.local"

    def get_local_dict(self):
        return {
            "_module": self.module,
            "root_path": ".",
        }

    def test_get_class(self):
        cls = get_engine_class(self.module, EngineRoles.producer)
        self.assertEqual(cls, LocalProducer)

        cls = get_engine_class(self.module, EngineRoles.consumer)
        self.assertEqual(cls, LocalConsumer)

    def test_from_dict(self):
        settings = self.get_local_dict()
        obj = instantiate_from_dict(settings, EngineRoles.producer)
        self.assertIsInstance(obj, LocalProducer)

    def test_from_path(self):
        obj = instantiate_from_path(SETTINGS_PATH, EngineRoles.producer)
        self.assertIsInstance(obj, LocalProducer)
