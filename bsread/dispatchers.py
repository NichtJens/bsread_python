import bsread.dispatcher as _dispatcher


def get_current_channels(base_urls):
    res = []
    for bu in base_urls:
        channels = _dispatcher.get_current_channels(base_url=bu)
        res.extend(channels)
    res.sort(key=lambda ch: ch["name"])
    return res



