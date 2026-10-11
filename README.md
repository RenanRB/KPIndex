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
| 2 | GFZ forecast | modelled per-bin Kp, 3 days out |
| 3 | NOAA 3-day | a real per-bin profile, 3 days out |
| 4 | NOAA 27-day | one figure per day, fills the tail |

That ordering is what makes the output survive an outage. A feed is dropped
whenever it stops reaching into the future, judged on the newest timestamp
inside the document rather than on its HTTP status — being served is not the
same as being maintained. The next source then takes its place.

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

## Kp is global, not regional

Kp is a *planetary* index: one number per three-hour bin for the whole Earth,
derived from 13 mid-latitude observatories. There is no Brazilian Kp or
European Kp — if an app shows different numbers per region, it is not showing
Kp as defined.

What genuinely does vary by place is the **local K index**, measured at a
single observatory on the same 0–9 scale. GFZ forecasts those too, and they
disagree with each other and with Kp for the same hour:

```
                       09:00  12:00  15:00  18:00
Kp GLOBAL (planetary)   2.00   2.00   1.33   1.00
K  UPS (Uppsala, SE)     2.0    3.0    3.0    1.0
K  BFE (Brorfelde, DK)   2.0    2.0    2.0    4.0
K  CNB (Canberra, AU)    3.0    3.0    1.0    1.0
```

Source: [`forecast_mean_bars_K.json`](https://isdc-data.gfz.de/geomagnetism/HpoForecast/v0102/output/H/json/forecast_mean_bars_K.json)
— 13 stations: MEA, SIT, LER, UPS, OTT, ESK, BFE, WNG, NGK, HAD, FRD, CNB, EYR.
**None is in South America**, so this product cannot give a local index for
Brazil, and there is no forecast of local K anywhere near it.

Brazil does have magnetometers in [INTERMAGNET](https://intermagnet.org/)
(VSS/Vassouras, TTB/Tatuoca), but two things rule them out as a drop-in here:
they publish *measurements*, not a forecast, and INTERMAGNET data is
[CC BY-NC 4.0](https://intermagnet.org/data_conditions.html) — commercial use,
sale or bulk distribution needs written permission from each operating
institute. Neither Brazilian observatory appears on the list of institutes with
broader terms. Everything this project publishes today comes from GFZ
(CC BY 4.0) and NOAA SWPC (US government work, public domain), both usable
commercially with attribution, so pulling in NC-licensed data would quietly
restrict the whole feed.

### Mixing local K into this feed would be a mistake

Local K and Kp share the 0–9 scale but are different quantities, so a feed
whose `kp` field sometimes meant one and sometimes the other would be
unreadable — a consumer could not tell which. Deriving local K from global Kp
is also an open research problem, not arithmetic: published approaches use
per-station conversion tables or neural networks, and the relationship shifts
with geomagnetic latitude and local time. Geographic nearness is the wrong
criterion anyway; a station at a similar geomagnetic latitude is more
representative than a closer one.

If local indices are ever wanted here, they belong in their own file with
their own field name, never blended into `kp`.

## Why only GFZ and NOAA

Checked while looking for redundancy, with what each actually returned:

| Source | Horizon | Verdict |
| --- | --- | --- |
| NOAA 27-day outlook | ~27 days | **the only 7-day Kp forecast found** |
| GFZ Hpo forecast | ~68 h | used, 3-day model consensus |
| NOAA 3-day forecast | ~72 h | used, per-bin profile |
| GFZ Kp app/json | measured | used, the realtime segment |
| NOAA `planetary_k_index_1m` | ~6 h of nowcast | 1-minute estimate that swings within a bin; not the consolidated Kp |
| NOAA `geospace_pred_est_kp` | ~1 h | too short to matter here |
| BGS, auroraforecast.space | — | 404 / TLS failure |

So **beyond three days there is no alternative to the NOAA 27-day outlook**.
That is a limit of the field, not of this project: the redundancy that exists
is in the 0–3 day range, which is why the window is filled by four feeds there
and by one at the tail.

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

- [GFZ](https://spaceweather.gfz.de/) — measured Kp, and the
  [Hpo forecast](https://spaceweather.gfz.de/products-data/observatory-based-geomagnetic-forecasts/forecasting-global-kp-and-hpo-indices)
  consensus of three models (CC BY 4.0)
- [NOAA SWPC](https://www.swpc.noaa.gov/) — 3-day geomagnetic forecast and 27-day outlook

> **Retired source.** Until October 2026 the GFZ forecast came from the
> PAGER/SWIFT CSV (`kp_product_file_FORECAST_PAGER_SWIFT_LAST.csv`). That file
> silently stopped updating on 3 Oct 2026 while still answering HTTP 200 — on
> both the `gfz-potsdam.de` and `gfz.de` hostnames — so it was replaced by the
> Hpo forecast rather than merely re-pointed. Note the Hpo URL carries a
> version segment (`v0102`); if GFZ bumps it, the freshness check will report
> the feed as frozen.

## Licence

**The code** in this repository is MIT — see [LICENSE](LICENSE).

**The data** in `data/` is not ours to relicense. It is derived from:

- **GFZ**, under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
  Attribution: *contains data from GFZ Helmholtz Centre for Geosciences, licensed
  under CC BY 4.0*. The data **has been modified**: bins are selected, snapped to
  the three-hour grid, merged across sources and re-serialised as JSON.
- **NOAA SWPC**, a work of the US federal government, in the public domain.

CC BY 4.0 requires attribution, a link to the licence and an indication of
changes, so anyone redistributing `data/` must carry the notice above. That is
why the MIT licence covers the code only — applying it to the data would
misstate the terms the GFZ grants them under.
