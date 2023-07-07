import click

from mkite_engines.cli.redis import redis


class MkiteEnginesGroup(click.Group):
    pass


@click.command(cls=MkiteEnginesGroup)
def kiteng():
    """Command line interface for mkite_core"""


kiteng.add_command(redis)

if __name__ == "__main__":
    kiteng()
