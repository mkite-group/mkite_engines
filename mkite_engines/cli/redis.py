import click

from mkite_engines.redis import RedisEngine


def summary(engine):
    queues = engine.list_queue_names()
    for q in queues:
        res = engine.list_queue(q)
        print(f"{q}: {len(res)}")


@click.command("redis")
@click.option(
    "-s",
    "--settings",
    type=str,
    default=None,
    help="path to the yaml file configuring the Redis engine",
)
def redis(settings):
    from IPython import embed

    engine = RedisEngine.from_file(settings)
    summary(engine)
    r = engine.r

    embed()
