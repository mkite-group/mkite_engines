import click

from mkite_engines.redis import RedisEngine


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
    r = engine.r

    embed()
