import asyncio
from typing import Any, cast

import aiohttp
import pytest
from aiohttp.client_reqrep import ConnectionKey
from src.capabilities import Outcome, ProbeOutcome
from src.validation import carries_expected_payload, classify, probe

# the connection a failed request was attempting, which is all these errors want
CANDIDATE_CONNECTION = ConnectionKey(
    "1.1.1.1",
    80,
    False,
    ssl=False,
    proxy=None,
    proxy_auth=None,
    proxy_headers_hash=None,
)


class FakeResponse:
    """A response a fake session hands back, without opening a socket"""

    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def text(self) -> str:
        return self._body


class FakeSession:
    """A session that returns one prepared answer, or raises on the request"""

    def __init__(self, answer: FakeResponse | Exception) -> None:
        self.answer = answer

    def get(self, url: str, **kwargs: Any) -> Any:
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


async def probe_with(
    answer: FakeResponse | Exception, candidate: str = "1.1.1.1:80"
) -> ProbeOutcome:
    return await probe(
        cast("aiohttp.ClientSession", FakeSession(answer)),
        candidate,
        "https://api.ipify.org",
        1.0,
    )


def test_the_expected_payload_is_a_bare_address() -> None:
    assert carries_expected_payload("203.0.113.7\n") is True


@pytest.mark.parametrize(
    "body",
    [
        "<html><body>Sign in to the hotel wifi</body></html>",
        "",
        '{"origin": "203.0.113.7"}',
        "203.0.113.7 not-an-address",
    ],
)
def test_anything_else_is_not_the_expected_payload(body: str) -> None:
    assert carries_expected_payload(body) is False


def test_a_response_carrying_the_expected_payload_is_working() -> None:
    assert classify(200, "203.0.113.7") is Outcome.WORKING


def test_a_plausible_200_with_the_wrong_body_is_rejected() -> None:
    assert classify(200, "<html>captive portal</html>") is Outcome.REJECTED


@pytest.mark.parametrize("status", [401, 403, 407, 429, 502])
def test_a_refusal_from_the_target_is_rejected(status: int) -> None:
    assert classify(status, "") is Outcome.REJECTED


async def test_a_working_probe_records_a_latency() -> None:
    outcome = await probe_with(FakeResponse(200, "203.0.113.7"))

    assert outcome.state is Outcome.WORKING
    assert outcome.latency_ms is not None
    assert outcome.proxy == "1.1.1.1:80"


async def test_a_probe_answering_with_the_wrong_body_records_no_latency() -> None:
    outcome = await probe_with(FakeResponse(200, "<html>captive portal</html>"))

    assert outcome.state is Outcome.REJECTED
    assert outcome.latency_ms is None


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError(),
        aiohttp.ServerTimeoutError(),
        aiohttp.ClientProxyConnectionError(
            CANDIDATE_CONNECTION, OSError("connection refused")
        ),
        aiohttp.ClientConnectorDNSError(
            CANDIDATE_CONNECTION, OSError("name or service not known")
        ),
    ],
)
async def test_a_probe_without_an_answer_is_unreachable(error: Exception) -> None:
    outcome = await probe_with(error)

    assert outcome.state is Outcome.UNREACHABLE
    assert outcome.latency_ms is None


async def test_a_refused_connection_says_so_in_its_reason() -> None:
    outcome = await probe_with(
        aiohttp.ClientProxyConnectionError(
            CANDIDATE_CONNECTION, OSError("connection refused")
        )
    )

    assert "ClientProxyConnectionError" in outcome.reason


async def test_a_timed_out_probe_says_so_in_its_reason() -> None:
    outcome = await probe_with(TimeoutError())

    assert "TimeoutError" in outcome.reason


async def test_a_wrong_payload_is_named_in_the_reason() -> None:
    outcome = await probe_with(FakeResponse(200, "<html>captive portal</html>"))

    assert "captive portal" in outcome.reason


async def test_a_target_that_is_not_a_url_fails_loudly() -> None:
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.InvalidURL):
            await probe(session, "1.1.1.1:80", "not-a-url", 1.0)


async def test_cancelling_a_probe_is_not_swallowed() -> None:
    class CancellingSession(FakeSession):
        def get(self, url: str, **kwargs: Any) -> Any:
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await probe(
            cast("aiohttp.ClientSession", CancellingSession(FakeResponse(200, ""))),
            "1.1.1.1:80",
            "https://api.ipify.org",
            1.0,
        )
