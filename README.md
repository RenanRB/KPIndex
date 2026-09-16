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
| [`data/kp.json`](data/kp.json) | GFZ measured + GFZ forecast + NOAA 27-day | ~2 days of past, ~9 days total |
| [`data/kp_noaa.json`](data/kp_noaa.json) | NOAA 3-day + NOAA 27-day | ~7 days ahead, forecast only |

Each file is a chronological JSON array of three-hour bins:

```json
[
    { "datetime": "2026-09-14T00:00:00Z", "kp": 1.0 },
    { "datetime": "2026-09-14T03:00:00Z", "kp": 2.0 }
]
```

- `datetime` — UTC, always on a real Kp bin (00, 03, 06, 09, 12, 15, 18, 21).
- `kp` — always a JSON number in the 0–9 range.

`kp.json` blends measurement with prediction: bins in the past carry observed
Kp, and a measured value always overrides a forecast for the same bin. The tail
of both files comes from the NOAA 27-day outlook, which publishes a single
figure per day — the largest Kp expected — repeated across that day's eight
bins. **The tail is an upper bound, not an hourly profile.**

## Guarantees

The pipelines are built so that publishing nothing beats publishing something
wrong. Before any file is written, the whole series must satisfy:

- every timestamp lands on a real three-hour bin;
- timestamps are strictly increasing, with no duplicates;
- every `kp` is a number within 0–9 (nulls and `-1` sentinels are dropped);
- the series is not suspiciously short.

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
series still reaches. The staleness budgets come from each file's measured
update cadence, which differs a lot between them:

| File | Median | Slowest observed | Budget |
| --- | --- | --- | --- |
| `kp.json` | 1.3 h | 11.8 h | 18 h |
| `kp_noaa.json` | 24.0 h | 48.0 h | 72 h |

## Layout

| Path | Purpose |
| --- | --- |
| `kp_core.py` | Shared fetching, Kp invariants and atomic writes |
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
