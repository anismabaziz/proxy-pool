from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    """A place proxies are published, and the name we file them under"""

    name: str
    url: str


SOURCES: tuple[Source, ...] = (
    Source("free-proxy-list", "https://free-proxy-list.net/"),
    Source("ssl-proxies", "https://sslproxies.org/"),
    Source("us-proxy", "https://www.us-proxy.org/"),
    Source("socks-proxy", "https://www.socks-proxy.net/"),
    Source(
        "proxy-list.download", "https://www.proxy-list.download/api/v1/get?type=http"
    ),
    Source(
        "github_raw_spys",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    ),
    Source(
        "github_raw_mtb",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    ),
    Source(
        "geonode",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
    ),
)
