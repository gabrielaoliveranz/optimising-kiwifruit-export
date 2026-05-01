# APOPHENIA Simulator — Frontend

The interactive executive dashboard. Vanilla HTML/CSS/JavaScript with Chart.js and Mapbox GL JS.

## Run locally

1. Configure your Mapbox token:
   ```bash
   cp config.local.example.js config.local.js
   ```
   Edit `config.local.js` and replace the placeholder with your real public token.

2. Serve from this directory:
   ```bash
   python -m http.server 8000
   ```

3. Open `http://localhost:8000/apophenia_v4_executive.html`

## Architecture

- `apophenia_v4_executive.html` — single-file application
- `config.local.js` — Mapbox token (gitignored, never committed)
- `assets/` — images (hero photo, author photo)

The application loads `payload_live.json` from `../07_reports/api_payloads/` for live data; falls back to hardcoded defaults if the payload is unavailable.

## Token security

- The Mapbox token in `config.local.js` is public but URL-restricted to allowed domains in the Mapbox dashboard
- Allowed URLs include `localhost:8000`, `localhost:5500`, and the deployed domain
- The token is never committed — `.gitignore` excludes `config.local.js`
- A template `config.local.example.js` is provided for new clones

## Browser support

Modern browsers with ES2015+ support. Tested on Chrome, Edge, Firefox.
