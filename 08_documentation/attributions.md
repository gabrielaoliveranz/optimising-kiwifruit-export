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

- "A bunch of green fruit hanging from a tree" by Niranjan Lamichhane — Unsplash (https://unsplash.com/photos/a-bunch-of-green-fruit-hanging-from-a-tree-6mxOodN6KHk), Unsplash License (free for commercial and non-commercial use, no permission needed; attribution not required but given here). Covers both `06_simulator/assets/hero-orchard.jpg` and `06_simulator/assets/hero-orchard.webp` — the same photograph in two formats, `.webp` a converted derivative of the `.jpg`, not a separate asset.

- jsPDF v2.5.1 by the jsPDF contributors — cdnjs CDN, loaded dynamically at runtime rather than a static `<script>` tag (`06_simulator/assets/js/app.js:1663`, `JSPDF_CDN = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js'`, injected via `sc.src = JSPDF_CDN` at `app.js:1752` when the PDF-export feature is actually used), MIT License

## Status

Six third-party assets logged: five CDN dependencies, verified against
`06_simulator/index.html` **and** `06_simulator/assets/js/app.js`, plus
the hero photograph. Four of the five CDN entries (Fraunces/Inter via
Google Fonts, Chart.js, and jsPDF) were not part of the original request
that prompted this file; they surfaced during verification passes and
are logged for the same reason the others were: used but previously
uncredited.

**Verified 2026-08-15, corrected from an earlier version of this
section:** this log's stated methodology used to say it was complete
after grepping only `index.html` for every `http(s)://` reference —
that missed jsPDF entirely, because it isn't a static `<script src>` in
the HTML at all, it's a URL string inside `app.js` that gets injected
into a `<script>` element only when a visitor actually exports a PDF.
Any dependency loaded programmatically from JS, not declared in markup,
will keep being invisible to an HTML-only grep. Re-verify against both
files (and any other JS entry point this project gains) next time this
log is audited, not `index.html` alone.

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
