import click
from bsread import dispatchers, utils
import re


@click.command()
@click.argument("pattern", default=".*")
@click.option("--all", "metadata", default=False, is_flag=True, help="Display meta information")
@click.option("--base_url", default=None, help="URL of dispatcher")
@click.option("--backend", default=None, help="Backend to query")
def avail(pattern=None, base_url=None, backend=None, metadata=False):

    base_urls = utils.get_base_urls(base_url, backend)

    pattern = ".*" + pattern + ".*"

    try:
        channels = dispatchers.get_current_channels(base_urls=base_urls)
        for channel in channels:
            if re.match(pattern, channel["name"]):
                if metadata:
                    click.echo(f"{channel['name']:50} {channel['type']} {channel['shape']} "
                               f"{channel['modulo']} {channel['offset']} {channel['source']}")
                else:
                    click.echo(channel["name"])
    except Exception as e:
        click.echo(f"Unable to retrieve channels\nReason:\n{e}", err=True)


def main():
    avail()


if __name__ == "__main__":
    avail()
