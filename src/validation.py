import time
from ipaddress import ip_address

import aiohttp

from src.capabilities import Outcome, ProbeOutcome

# a timeout, a refused connection, a name that would not resolve, a transfer that
# broke halfway: all of them are the same answer, which is that no usable
# response came back
CONNECTIVITY_FAILURES = (TimeoutError, OSError, aiohttp.ClientError)

# a target that is not a URL is a mistake in the settings rather than a fact
# about the candidate, so it is left to fail loudly
MISCONFIGURATION = (aiohttp.InvalidURL, aiohttp.NonHttpUrlClientError)


def carries_expected_payload(body: str) -> bool:
    """Whether the body is the bare address the validation target returns, as
    opposed to an error page or a sign-in portal dressed up as a 200"""

    try:
        ip_address(body.strip())
    except ValueError:
        return False

    return True


def classify(status: int, body: str) -> Outcome:
    """The state a response proves. A 200 carrying the wrong body is not working:
    the target did answer, and what it said is not what we asked for. Any other
    answer is a refusal, either by the target or by a proxy that could not
    reach it"""

    if status == 200 and carries_expected_payload(body):
        return Outcome.WORKING

    return Outcome.REJECTED


def refusal(status: int, body: str) -> str:
    """What a response said instead of the expected payload"""

    if status == 200:
        return f"unexpected payload: {body.strip()[:60]!r}"

    return f"HTTP {status}"


async def probe(
    session: aiohttp.ClientSession, candidate: str, target_url: str, timeout_s: float
) -> ProbeOutcome:
    """Put one candidate through a request to the validation target, and say
    whether it carried it, could not be reached, or was refused"""

    start = time.perf_counter()

    try:
        async with session.get(
            target_url,
            proxy=f"http://{candidate}",
            timeout=aiohttp.ClientTimeout(total=timeout_s),
        ) as resp:
            body = await resp.text()
            state = classify(resp.status, body)
            elapsed_ms = int((time.perf_counter() - start) * 1000)

            return ProbeOutcome(
                proxy=candidate,
                state=state,
                latency_ms=elapsed_ms if state is Outcome.WORKING else None,
                reason="" if state is Outcome.WORKING else refusal(resp.status, body),
            )

    except MISCONFIGURATION:
        raise

    except CONNECTIVITY_FAILURES as failure:
        # no answer came back at all, so there is no latency to report: a
        # measured time for a request that never completed is not a latency
        return ProbeOutcome(
            proxy=candidate,
            state=Outcome.UNREACHABLE,
            reason=type(failure).__name__,
        )
