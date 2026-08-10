# Attributions

Log of third-party assets and CDN dependencies used in this project,
verified against the actual codebase — not reconstructed from memory —
matching the format of the sister repo's
`horticultural-land-suitability-nz/docs/attributions.md`.

Format:

```markdown
- <Asset name> by <Author/Vendor> — <Source> (<URL>), <Licence>
```

## Log

- Phosphor Icons v2.0.3 by Phosphor Icons — unpkg CDN (`06_simulator/index.html:22`, https://unpkg.com/@phosphor-icons/web@2.0.3/src/regular/style.css), MIT License

- Mapbox GL JS v3.7.0 by Mapbox — Mapbox CDN (`06_simulator/index.html:23-24`, https://api.mapbox.com/mapbox-gl-js/v3.7.0/), used under the Mapbox Terms of Service and a Mapbox access token — **not** an open-source licence: Mapbox GL JS moved off BSD-style licensing at v2.0 (December 2020); this project is on v3.7.0

- Chart.js v4.4.1 by the Chart.js contributors — cdnjs CDN (`06_simulator/index.html:21`, https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js), MIT License

- Fraunces and Inter by their respective type designers — Google Fonts (`06_simulator/index.html:19`, https://fonts.googleapis.com/css2?family=Fraunces...&family=Inter...), SIL Open Font License 1.1

## Status

Four CDN dependencies logged, verified against `06_simulator/index.html`
directly (grepped for every `http(s)://` reference in that file — these
four are the complete set). Two of the four (Fraunces/Inter via Google
Fonts, and Chart.js) were not part of the original request that prompted
this file; they surfaced during the same verification pass and are
logged for the same reason the other two were: used but previously
uncredited.

**Unresolved — do not guess:** `assets/hero-orchard.webp` and
`assets/hero-orchard.jpg`'s provenance and licence are not established.
They are not logged above because their status is genuinely unknown, not
because they were checked and found to need no entry. Do not assume they
are original photography or public domain; do not assume they need a
Flaticon-style credit either. This needs the asset's actual source
before an entry can be written.

Not logged, and not third-party: `assets/gabriela.webp`,
`assets/preview/hero.png`, `04_analysis/star_schema/apophenia-star-schema.{png,svg}`,
and `07_reports/presentation_slides/apophenia_dashboard_screenshot.png`
appear to be this project's own original photography/diagrams/screenshots,
not third-party assets — outside this log's scope, same as the sister
repo's own attributions.md only logs genuine third-party sources.

The six Flaticon credits previously in `README.md` (trust, stopwatch,
water, crate, money, clipboard icons) named files that do not exist
anywhere in this repository and have been removed, not migrated here —
see `README.md`'s Icon Credits section.
