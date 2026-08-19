# n8n workflow — Live ETL Feed

`apophenia_live_feed.json` is an exported n8n workflow definition. On a
daily cron (`0 18 * * *`, i.e. 18:00 UTC / 06:00 NZT the following
morning) it runs `03_etl_pipeline/api_feed.py --live --live-apis`, parses
the resulting `payload_live.json`, and — depending on the reported
`data_quality.health_score` — appends a success or a quality-alert entry
to `07_reports/api_payloads/etl_log.txt`.

## Setup

The workflow contains no machine-specific paths. Instead, its four
path-dependent nodes ("Run ETL Feed", "Parse Payload", "Log Success",
"Log Quality Alert") read the environment variable
`APOPHENIA_PROJECT_ROOT` at runtime.

Before importing this workflow into an n8n instance:

1. Set `APOPHENIA_PROJECT_ROOT` on that instance to the absolute path of
   this repository's root (e.g.
   `C:\Users\<you>\Proyectos\optimising-kiwifruit-export`).
2. Import `apophenia_live_feed.json` via n8n's "Import from File" option.
3. Activate the workflow once the environment variable is confirmed set
   — each node throws an explicit error if it's missing, rather than
   silently writing to the wrong place.

## Status

Exported workflow definition. It has never run: `active` is `false` in
the definition, and `etl_log.txt` — which both the success and the
quality-alert branches write to on every execution — does not exist
anywhere in the repo. The workflow automates the manual step
`python 03_etl_pipeline/api_feed.py --live --live-apis` on a daily 18:00
UTC cron. Scaffolding, not a live integration.
