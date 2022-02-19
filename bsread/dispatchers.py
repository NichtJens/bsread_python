import bsread.dispatcher as _dispatcher


def get_current_channels(base_urls):
    res = _collect(_dispatcher.get_current_channels, base_urls)
    res.sort(key=lambda ch: ch["name"])
    return res


def _collect(func, args):
    res = []
    for a in args:
        this = func(a)
        res.extend(this)
    return res



