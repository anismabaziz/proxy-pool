# proxy-pool

Public proxy lists publish thousands of addresses a day. This tool scrapes the
lists, sends a request through every address they offer, and keeps the ones that
carried it.

**The measured finding is not in yet.** The runs that would support a number are
still happening, so no figure here is the finding. What follows is one run's
raw record, straight from the tool, and it is on the page to show what the
output looks like rather than to stand in for a week of runs. [The run
history](#run-history) is the in-progress report, and the headline goes here
as soon as the runs are done. I would rather write "I do not know yet" than a
number I have not measured.

## What a run does

```
  sources          candidates        outcomes           pool           history
  ---------        ----------        --------           ----           --------
  8 lists   -->   ~66k ip:port  -->  working        -->  SQLite    -->  one row
  fetched         deduplicated,       unreachable          3 strikes      per run
  in turn         budget applied     rejected            + staleness
```

Four stages, and each one owns a word the rest of the code borrows.

**Scrape.** Every source declares its payload format up front, so the reading of
it lives next to the declaration rather than in a shared guesser. A source that
answers but offers nothing raises an error, because a source that has stopped
answering must not be able to look like a quiet one.

**Probe.** One HTTPS request to a target that echoes the caller's address,
sent through the candidate and timed from dispatch to response. A probe resolves
to one of three outcomes: working, unreachable, or rejected. A rejection is a
definitive answer that the proxy cannot serve, which is a different thing from
silence, and the two are never counted as the same.

**Retention.** A candidate that goes unreachable three times running leaves the
pool. A working probe clears its strikes. A rejection leaves them exactly where
they were, because a refusal is an answer rather than an absence of one. A
candidate no recent run probed also leaves, because an unprobed row is not data.

**Record.** Once the run is over it writes a row: when the run started, what each
source offered, and how many probes ended in each state. A run that fell over
records nothing, since a row claiming otherwise would be a finding the tool
never made.

## Run it

From a clean checkout:

```bash
git clone https://github.com/anismabaziz/proxy-pool.git
cd proxy-pool
uv sync --group dev
uv run python -m src scrape
```

A bare invocation prints usage and stops, rather than quietly starting a run
that rewrites the pool. The three subcommands are:

| Command | What it does |
| --- | --- |
| `scrape` | fetch every source, probe what came back, record the run |
| `revalidate` | probe the pool that already exists and let each candidate in or out |
| `report` | read the recorded runs back as hit rate, yield, and survival |

Every tuned value is a flag, so you can see what a run is doing and change it:
`--max-candidates`, `--chunk-size`, `--connector-limit`, `--probe-timeout`, and
the rest. `python -m src scrape --help` lists them.

## A real run

Recorded from this machine on 2026-09-27, the whole thing unedited. The budget is
capped at 300 candidates so the run finishes in under a minute; drop
`--max-candidates` for the full set.

```
$ uv run python -m src scrape --max-candidates 300
[SCRAPED] INFO    free-proxy-list: found 300 candidates
[SCRAPED] INFO    geonode: found 500 candidates
[SCRAPED] INFO    github_raw_spys: found 2463 candidates
[SCRAPED] INFO    github_raw_mtb: found 96 candidates
[SCRAPED] INFO    github_raw_ercindedeoglu: found 64498 candidates
[SCRAPED] INFO    github_raw_proxifly: found 5234 candidates
[SCRAPED] INFO    github_raw_jetkai: found 1801 candidates
[SCRAPED] INFO    github_raw_vakhov: found 524 candidates
[RAW] INFO    Total unique candidates scraped: 66012
[VALID] INFO    Chunk 1: Working 1 of 300
[POOL] INFO    Working: 1
[POOL] INFO    Left on three strikes: 0
[POOL] INFO    Left unprobed: 0
[FINISH] INFO    Probed 300 candidates
```

One address out of 300, drawn one from each source in turn rather than the first
300 off the top of one list. The share that worked is not in yet, and this run is
not where it comes from.

## Run history

`uv run python -m src report` reads the recorded runs back. This is the record
so far, one run in, unedited:

```
# Run history

## Hit rate per run

| Run | Started | Scraped | Candidates | Working | Unreachable | Rejected | Hit rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 2026-09-27 21:32 | 66012 | 300 | 1 | 299 | 0 | 0.3% |

## Yield per source

What each source got measured in, and how much of it worked. A run draws its candidates in turn and stops at its budget, so a source crowded out of a run is not in that run's yield.

| Source | Runs | Candidates | Working | Yield | Standing |
| --- | --- | --- | --- | --- | --- |
| free-proxy-list | 1 | 265 | 0 | 0.0% | nothing worked |
| geonode | 1 | 490 | 0 | 0.0% | nothing worked |
| github_raw_ercindedeoglu | 1 | 58623 | 0 | 0.0% | nothing worked |
| github_raw_jetkai | 1 | 1775 | 0 | 0.0% | nothing worked |
| github_raw_mtb | 1 | 93 | 1 | 1.1% | yielded |
| github_raw_proxifly | 1 | 2320 | 0 | 0.0% | nothing worked |
| github_raw_spys | 1 | 2088 | 0 | 0.0% | nothing worked |
| github_raw_vakhov | 1 | 358 | 0 | 0.0% | nothing worked |

## Survival

Not measured yet: the recorded runs span 0 minutes, and a figure needs at least a day.
```

Survival needs at least a day of runs behind it, so it stays empty until the
runs have spanned one. An empty figure is left empty rather than filled with a
zero, which would read as a measurement.

Read the yield column carefully. A source's yield divides by everything that
source offered, and this run probed 300 candidates out of the 66012 it scraped.
Most of those zeros mean nobody has probed those addresses yet, not that they
were probed and refused. The column becomes worth something once the runs have
covered the candidates they count.

## What the original source list got wrong

Three of the original eight sources were subpages of a single host.
`sslproxies.org`, `us-proxy.org`, and `socks-proxy.net` all resolve to the same
table that `free-proxy-list.net` serves, so the run was scraping one host's data
four times and calling it four sources. The damage was not the wasted requests.
It was that any per-source comparison drawn from those rows was measuring one
list against itself, which reads perfectly well as a table and means nothing.

They are one source now. A candidate two sources offer is counted once, against
whichever source the run drew it first, so an overlap shows up as a source
answering with nothing that was its own.

Two more defects came out of the same pass. `proxy-list.download` returned 502
for everything and was dropped. And geonode publishes address and port as
separate JSON fields, so the old extractor matched nothing while the run still
reported success: 500 candidates scraped, none found. It contributes candidates
now.

## Development

```bash
make check
```

That runs the linter, the type checker in strict mode, the tests, and the
formatter check. The tests never open a socket: a run is driven end to end
against a fake fetch and a fake probe, which is what made every change after
that one verifiable in one place.

No coverage threshold is enforced. A number becomes a target, and a suite is
better judged by whether the important paths are in it.

## License

MIT. See [LICENSE](LICENSE).
