from bsread import dispatcher as _dispatcher
from bsread.utils import DEFAULT_DISPATCHER_URLS


def get_current_channel_names(base_urls=DEFAULT_DISPATCHER_URLS):
    res = _collect(_dispatcher.get_current_channel_names, base_urls)
    res.sort()
    return res


def get_current_channels(base_urls=DEFAULT_DISPATCHER_URLS):
    res = _collect(_dispatcher.get_current_channels, base_urls)
    res.sort(key=lambda ch: ch["name"])
    return res


def _collect(func, args):
    res = []
    for a in args:
        this = func(a)
        res.extend(this)
    return res


def request_streams(channels, base_urls=DEFAULT_DISPATCHER_URLS):
    chans_map = _split_channels_by_backend(channels, base_urls)
    res = {}
    for bu, chans in chans_map.items():
#        print(bu, chans) #TODO: add logging
        src = _dispatcher.request_stream(chans, base_url=bu)
        res[bu] = src
#    print("streams:", res) #TODO: add logging
    return res


def _split_channels_by_backend(channels, base_urls):
    channels = set(channels)
    res = {}
    for bu in base_urls:
        current = _dispatcher.get_current_channel_names(base_url=bu)
        current = channels.intersection(current)
        if not current:
            continue

        res[bu] = current

    return res


def remove_streams(sources):
    for bu, src in sources.items():
        _dispatcher.remove_stream(src, base_url=bu)



