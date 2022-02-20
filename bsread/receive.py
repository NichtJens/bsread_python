import click
import mflow
from bsread.handlers.compact import Handler
from bsread import dispatchers, utils
import zmq
import numpy


def receive_many(sources, **kwargs):
    receivers = []
    for src in sources.values():
        rec = receive(source=src, **kwargs)
        receivers.append(rec)
    for _ in zip(*receivers):
        pass


def receive(source=None, clear=False, queue_size=100, mode=zmq.PULL, channel_filter=None):
    numpy.set_printoptions(threshold=5)
    numpy.set_printoptions(linewidth=100)

    print('Trying to connect to %s' % source)

    receiver = mflow.connect(source, conn_type="connect", queue_size=queue_size, mode=mode, receive_timeout=10)
    handler = Handler()

    while True:
        message = receiver.receive(handler=handler.receive)
        if message is None: # ran into timeout ...
            yield           # use old-school coroutine approach to run several receives that take turns
            continue        # ... try another receive

        message = message.data  # As the rest of the code is only interested in the message data, not statistics

        # if message_data['header']['pulse_id'] % 10 == 0:
        #     sys.stderr.write("\x1b[2J\x1b[H")

        if clear:
            print((chr(27) + "[2J"))

        separator = '\t'
        # separator = ', '

        if message.format_changed or clear:
            # Have pulse_id, ... in first column
            keys = "pulse_id" + separator + "global_timestamp" + separator + "global_timestamp_offset"

            for key in message.data.keys():

                if channel_filter and key not in channel_filter:
                    # continue with next element as this one is not in the filter list
                    continue

                if keys:
                    keys = keys + separator + key
                else:
                    keys = key

            print(keys)

        # pprint.pprint(message.data.values())
        # Have pulse_id in first column
        values = str(message.pulse_id) + separator + str(message.global_timestamp) + separator + str(
            message.global_timestamp_offset)

        for key in message.data.keys():

            if channel_filter and key not in channel_filter:
                # continue with next element as this one is not in the filter list
                continue

            value = message.data[key]
            if values:
                values = values + separator + str(value.value)
            else:
                values = str(value.value)

        # for value in message.data.values():
        #
        #     if values:
        #         values = values + separator + str(value.value)
        #     else:
        #         values = str(value.value)

        print(values)
        yield # use old-school coroutine approach to run several receives that take turns


@click.command()
@click.argument("channels", default=None, type=str, nargs=-1)
@click.option("-s", "--source", default=None, type=str, help="Source address - format 'tcp://<address>:<port>'")
@click.option("-m", "--mode", default="sub",
              type=click.Choice(["pull", "sub"], case_sensitive=False),
              help="Communication mode - either pull or sub (default depends on the use of -s option)")
@click.option("--clear", default=False, is_flag=True, help="Monitor mode / clear the screen on every message")
@click.option("-q", "--queue", "queue_size", default=100, type=int, help="Queue size of incoming queue")
@click.option("--base_url", default=None, help="URL of dispatcher")
@click.option("--backend", default=None, help="Backend to query")
def receive_(channels, source, mode, clear, queue_size, base_url, backend):

    base_urls = utils.get_base_urls(base_url, backend)

    mode = mflow.SUB if mode == 'sub' else mflow.PULL
    use_dispatching = False
    channel_filter = None

    if not channels and source is None:
        raise click.BadArgumentUsage("No source or channels are specified")

    if source:
        source = utils.check_and_update_uri(source, exception=click.BadArgumentUsage)
        base_url = dispatchers.assign_backend_to_source(source, base_urls)
        sources = {base_url: source}
        if channels:
            channel_filter = channels
    else:
        # Connect via the dispatching layer
        use_dispatching = True
        sources = dispatchers.request_streams(channels, base_urls=base_urls)

    try:
        receive_many(sources, clear=clear, queue_size=queue_size, mode=mode, channel_filter=channel_filter)

    except KeyboardInterrupt:
        # KeyboardInterrupt is thrown if the receiving is terminated via ctrl+c
        # As we don't want to see a stacktrace then catch this exception
        print() # print ^C on its own line
    finally:
        if use_dispatching:
            print('Closing stream')
            dispatchers.remove_streams(sources)


def main():
    receive_()


if __name__ == "__main__":
    main()
