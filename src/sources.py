import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import assert_never

ADDRESS = re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}\b")


class EmptySourceError(Exception):
    """A source that answered but offered no candidate at all"""


def address_list(text: str) -> list[str]:
    """Every ip:port pair in a page or plain-text list, in the order it lists them"""

    return list(dict.fromkeys(ADDRESS.findall(text)))


def geonode_json(text: str) -> list[str]:
    """Every ip:port pair in a geonode listing, whose address and port arrive as
    separate fields rather than as one address"""

    try:
        listing = json.loads(text)
    except ValueError:
        # an error page or an empty body is a source that offered nothing
        return []

    if not isinstance(listing, dict) or not isinstance(listing.get("data"), list):
        return []

    return list(
        dict.fromkeys(
            f"{record['ip']}:{record['port']}"
            for record in listing["data"]
            if isinstance(record, dict) and {"ip", "port"} <= record.keys()
        )
    )


class PayloadFormat(Enum):
    """The payload shapes we know how to read, each read by its own parser"""

    ADDRESS_LIST = "address-list"
    GEONODE_JSON = "geonode-json"

    def parse(self, text: str) -> list[str]:
        match self:
            case PayloadFormat.ADDRESS_LIST:
                return address_list(text)
            case PayloadFormat.GEONODE_JSON:
                return geonode_json(text)
            case _:
                assert_never(self)


@dataclass(frozen=True)
class Source:
    """A place proxies are published, the name we file them under, and the
    shape of the payload it hands back"""

    name: str
    url: str
    payload_format: PayloadFormat

    def scrape(self, text: str) -> list[str]:
        """Every candidate this payload offers, or an error when it offers none"""

        candidates = self.payload_format.parse(text)

        if not candidates:
            raise EmptySourceError(
                f"{self.name} returned no candidates from {self.url}"
            )

        return candidates


SOURCES: tuple[Source, ...] = (
    Source(
        "free-proxy-list",
        "https://free-proxy-list.net/",
        PayloadFormat.ADDRESS_LIST,
    ),
    Source(
        "geonode",
        "https://proxylist.geonode.com/api/proxy-list"
        "?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
        PayloadFormat.GEONODE_JSON,
    ),
    Source(
        "github_raw_spys",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        PayloadFormat.ADDRESS_LIST,
    ),
    Source(
        "github_raw_mtb",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        PayloadFormat.ADDRESS_LIST,
    ),
    Source(
        "github_raw_ercindedeoglu",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        PayloadFormat.ADDRESS_LIST,
    ),
    Source(
        "github_raw_proxifly",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list"
        "/main/proxies/protocols/http/data.txt",
        PayloadFormat.ADDRESS_LIST,
    ),
    Source(
        "github_raw_jetkai",
        "https://raw.githubusercontent.com/jetkai/proxy-list"
        "/main/online-proxies/txt/proxies-http.txt",
        PayloadFormat.ADDRESS_LIST,
    ),
    Source(
        "github_raw_vakhov",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        PayloadFormat.ADDRESS_LIST,
    ),
)
