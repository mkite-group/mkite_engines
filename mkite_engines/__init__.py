from .base import BaseEngine, BaseProducer, BaseConsumer, EngineRoles
from .local import LocalEngine, LocalProducer, LocalConsumer
from .redis import RedisEngine, RedisProducer, RedisConsumer
from .instantiate import get_engine_class, instantiate_from_dict, instantiate_from_path

PUBLISHERS = {
    "local": LocalProducer,
    "redis": RedisProducer,
}

CONSUMERS = {
    "local": LocalConsumer,
    "redis": RedisConsumer,
}

ENGINES = {
    "local": LocalEngine,
    "redis": RedisEngine,
}
