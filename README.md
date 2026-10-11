# KPIndex

Hourly, machine-readable feed of the planetary geomagnetic activity index (Kp),
published as JSON so an application can consume it without scraping anything.

```
https://raw.githubusercontent.com/RenanRB/KPIndex/main/data/kp.json
```

## The data

Two files are published, both refreshed hourly by GitHub Actions:

| File | Sources | Coverage |
| --- | --- | --- |
| [`data/kp.json`](data/kp.json) | GFZ measured + GFZ forecast + NOAA 3-day + NOAA 27-day | 2 days of past, 7 days ahead |
| [`data/kp_noaa.json`](data/kp_noaa.json) | NOAA 3-day + NOAA 27-day | 7 days ahead, forecast only |

Both windows are anchored on the current time, not on the data, so the span is
the same on every run.

Each file is a chronological JSON array of three-hour bins:

```json
[
    { "datetime": "2026-09-14T00:00:00Z", "kp": 1.0 },
    { "datetime": "2026-09-14T03:00:00Z", "kp": 2.0 }
]
```

- `datetime` — UTC, always on a real Kp bin (00, 03, 06, 09, 12, 15, 18, 21).
- `kp` — always a JSON number in the 0–9 range.

### How a bin is chosen

Several feeds overlap, so each bin is taken from the most trustworthy source
that offers it:

| Priority | Source | What it gives |
| --- | --- | --- |
| 1 | GFZ realtime | Kp already measured — wins outright |
| 2 | GFZ forecast | modelled per-bin Kp, a few days out |
| 3 | NOAA 3-day | a real per-bin profile, 3 days out |
| 4 | NOAA 27-day | one figure per day, fills the tail |

That ordering is what makes the output survive an outage: a feed that freezes
stops offering bins inside the window and the next source takes over.

The NOAA 27-day outlook publishes a single figure per day — the largest Kp
expected — repeated across that day's eight bins. **Where the tail comes from
it, it is an upper bound, not an hourly profile.**

## Guarantees

The pipelines are built so that publishing nothing beats publishing something
wrong. Before any file is written, the whole series must satisfy:

- every timestamp lands on a real three-hour bin;
- timestamps are strictly increasing, with no duplicates;
- every `kp` is a number within 0–9 (nulls and `-1` sentinels are dropped);
- the series is not suspiciously short;
- the series reaches at least 3 days into the future.

That last rule is deliberate: a series that no longer covers the days ahead is
worse than the file it would replace, so the run keeps the old one and says so.

If a check fails the run aborts and the previous file is left untouched. The
same checks run in CI against the committed files, so bad data cannot sit in
the repository unnoticed.

Runs are also tolerant of the upstream feeds, which time out fairly often:
each request is retried with exponential backoff, and if a source is still
unreachable the run finishes green with a warning rather than overwriting good
data with a partial series.

That tolerance would otherwise create a blind spot — a feed down for days
would leave the files quietly frozen — so a separate job runs four times a day
and **fails loudly** once the data has genuinely stopped moving. It checks two
things: how long since each file last changed, and how far beyond now the
series still reaches. It also probes the feeds themselves and flags any that
have stopped moving forward, which is what catches a frozen upstream on day
one rather than once the published files have decayed into the threshold.

The staleness budgets come from each file's measured update cadence, which
differs a lot between them:

| File | Median | Slowest observed | Budget |
| --- | --- | --- | --- |
| `kp.json` | 1.3 h | 11.8 h | 18 h |
| `kp_noaa.json` | 24.0 h | 48.0 h | 72 h |

## Layout

| Path | Purpose |
| --- | --- |
| `kp_core.py` | Shared fetching, Kp invariants, windowing and atomic writes |
| `noaa.py` | Parsers for the two NOAA SWPC products |
| `search.py` | Builds `data/kp.json` (GFZ + NOAA) |
| `search_noaa.py` | Builds `data/kp_noaa.json` (NOAA only) |
| `validate_data.py` | Audits published files; used as the CI gate |
| `check_freshness.py` | Fails when the data has stopped being updated |
| `fixtures/` | Verbatim captures of the real feeds, for tests |

## Development

```bash
pip install -r requirements.txt

python -m unittest discover -p 'test_*.py' -v   # run the tests
python validate_data.py                         # audit data/
python check_freshness.py                       # is the data still moving?
python search.py                                # writes new_kp.json
```

Tests never touch the network: they run against the captured documents in
`fixtures/`. To refresh those captures, download each URL listed at the top of
`search.py` and `search_noaa.py` over the matching file.

## Sources

- [GFZ Potsdam](https://spaceweather.gfz-potsdam.de/) — measured Kp and short-term forecast (CC BY 4.0)
- [NOAA SWPC](https://www.swpc.noaa.gov/) — 3-day geomagnetic forecast and 27-day outlook

## Licence

MIT — see [LICENSE](LICENSE).
