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



