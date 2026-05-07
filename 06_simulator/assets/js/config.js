/*
=============================================================================
APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
Bay of Plenty Corridor · Independent Research Project
File: config.js
Role: Domain configuration — ALL supply-chain-specific values live here.
      Swap this file to port the risk engine to a different commodity.
Author: Gabriela Olivera | Data Analytics Portfolio
Version: 4.1.0 | 2026-05
=============================================================================
*/

/**
 * Single source of truth for every domain-specific value in the application.
 *
 * ARCHITECTURE CONTRACT
 * ─────────────────────
 * app.js (core engine) must never contain hardcoded business strings, geographic
 * coordinates, or domain constants. If app.js needs a value, it reads it here.
 * Replacing this object with a different domain configuration (e.g., dairy logistics,
 * wine export, fisheries) must not require any changes to app.js.
 *
 * @type {Readonly<Object>}
 */
export const CONFIG = Object.freeze({

  /* ── Project identity ─────────────────────────────────────────── */
  meta: {
    title:    'APOPHENIA',
    subtitle: 'Horticultural Export Risk Intelligence Agent',
    corridor: 'Bay of Plenty Corridor',
    version:  '4.1.0',
    season:   '2025/26',
    modelId:  'APO v4'
  },

  /* ── Chart.js colour palette ──────────────────────────────────── */
  theme: {
    green:       '#003d2b',
    greenLight:  'rgba(0,61,43,.08)',
    greenHi:     '#00674a',
    greenMid:    '#6db38e',
    gold:        '#c9a961',
    goldLight:   'rgba(201,169,97,.10)',
    orange:      '#b45309',
    red:         '#991b1b',
    border:      '#e8ebe6',
    text:        '#4a5550',
    dim:         '#8a938d',
    ink:         '#0e1410',
    fontSans:    "'Inter', sans-serif",
    fontMono:    "'JetBrains Mono', monospace"
  },

  /* ── API / data sources ───────────────────────────────────────── */
  api: {
    /* Ordered list of candidate paths for the live ETL payload.
       The engine tries each in sequence and uses the first that resolves. */
    payloadPaths: [
      './payload_live.json',
      '../07_reports/api_payloads/payload_live.json',
      '/payload_live.json'
    ],
    mapbox: {
      center:     [176.55, -37.82],
      zoom:       8.6,
      style:      'mapbox://styles/mapbox/light-v11',
      trafficUrl: 'mapbox://mapbox.mapbox-traffic-v1'
    }
  },

  /* ── Risk model parameters ────────────────────────────────────── */
  model: {
    /**
     * Input clamp bounds — enforced on every external value before it
     * reaches the risk calculation functions. Changing a bound here
     * automatically propagates to slider validation and DOM rendering.
     */
    bounds: {
      dm:      { min: 14.0, max: 20.0 },
      pest:    { min: 0,    max: 100  },
      cong:    { min: 0,    max: 100  },
      rain:    { min: 0,    max: 120  },
      vol:     { min: 50,   max: 150  },
      reg:     { min: 0,    max: 100  },
      dwell:   { min: 6,    max: 48   },
      cyclone: { min: 0,    max: 100  },
      frost:   { min: 0,    max: 100  }
    },

    /**
     * Grower payment tier thresholds and rates (NZD).
     * Source: Grower Payments Booklet 2026.
     */
    paymentTiers: [
      { id: 'fail',      label: 'MTS Fail',          dmMax: 15.5,  submit: 0,    taste: 0,    total: 0,    rate: '$0.00/tray' },
      { id: 'green',     label: 'Green MTS',          dmMax: 16.1,  submit: 3.20, taste: 0,    total: 3.20, rate: '$3.20/tray' },
      { id: 'sungold',   label: 'SunGold G3 Premium', dmMax: 17.0,  submit: 3.60, taste: 0.53, total: 4.13, rate: '$4.13/tray' },
      { id: 'ultra',     label: 'SunGold Ultra',      dmMax: Infinity, submit: 3.60, taste: 0.80, total: 4.40, rate: '$4.40/tray' }
    ],

    /**
     * Historical season presets for scenario replay.
     * The 'live' entry is the default state; all other entries replay named seasons.
     */
    seasons: {
      live:      { note: '',                                                                                            dm: 16.2, cong: 25, rain: 18,  vol: 100, pest: 20, reg: 15, dwell: 18 },
      '2025/26': { note: '2025/26 — above-average dry matter, moderate congestion.',                                  dm: 16.8, cong: 30, rain: 22,  vol: 105, pest: 18, reg: 14, dwell: 20 },
      '2024/25': { note: 'Warning: 2024/25 — Cyclone Gabrielle impact. Elevated pest pressure.',                      dm: 15.8, cong: 55, rain: 85,  vol: 88,  pest: 45, reg: 22, dwell: 28 },
      '2023/24': { note: 'Record season 2023/24 — peak dry matter, optimal logistics.',                               dm: 17.4, cong: 18, rain: 12,  vol: 118, pest: 10, reg: 10, dwell: 14 },
      '2022/23': { note: '2022/23: Baseline season, standard operating conditions.',                                  dm: 16.0, cong: 28, rain: 25,  vol: 96,  pest: 22, reg: 16, dwell: 19 }
    }
  },

  /* ── Slider definitions ───────────────────────────────────────── */
  /**
   * Each slider definition drives both the HTML control and the model input.
   * id must match the corresponding key in model.bounds and the state object.
   */
  sliders: [
    {
      id:           'dm',
      category:     'Fruit Quality',
      label:        'Dry Matter — Pool Composite',
      ariaLabel:    'Dry Matter percentage, 14 to 20 percent',
      unit:         '%',
      defaultValue: 16.2,
      ticks:        ['14%', 'MTS 15.5%', '17.2%', '20%'],
      defaultBadge: 'Baseline — Premium active'
    },
    {
      id:           'pest',
      category:     'Fruit Quality',
      label:        'Pest Pressure — Phytosanitary Index',
      ariaLabel:    'Pest Pressure phytosanitary index, 0 to 100 percent',
      unit:         '%',
      defaultValue: 20,
      ticks:        ['None', 'CCP2', 'CCP3', 'Quarantine'],
      defaultBadge: 'CCP2 monitoring'
    },
    {
      id:           'cong',
      category:     'Logistics',
      label:        'SH2 Corridor Congestion — Tauranga',
      ariaLabel:    'SH2 Corridor Congestion, 0 free flow to 100 blocked',
      unit:         '%',
      defaultValue: 25,
      ticks:        ['Free flow', 'Moderate', 'Heavy', 'Blocked'],
      defaultBadge: 'Normal traffic'
    },
    {
      id:           'rain',
      category:     'Logistics',
      label:        'BOP Rainfall Forecast — Open-Meteo 7d',
      ariaLabel:    'BOP Rainfall forecast, 0 to 120 millimetres',
      unit:         ' mm',
      defaultValue: 18,
      ticks:        ['0mm', '40mm', '80mm', '120mm'],
      defaultBadge: 'Dry conditions'
    },
    {
      id:           'vol',
      category:     'Supply Chain',
      label:        'Export Volume Index — Crop size',
      ariaLabel:    'Export Volume Index, 50 to 150 million trays',
      unit:         ' M trays',
      defaultValue: 100,
      ticks:        ['50M', '83M', '117M', '150M'],
      defaultBadge: 'On forecast'
    },
    {
      id:           'reg',
      category:     'Supply Chain',
      label:        'Regulatory Compliance Load — MPI',
      ariaLabel:    'Regulatory Compliance Load, 0 compliant to 100 blocked',
      unit:         '%',
      defaultValue: 15,
      ticks:        ['Compliant', 'Low risk', 'MPI Flag', 'Blocked'],
      defaultBadge: 'Fully compliant'
    },
    {
      id:           'dwell',
      category:     'Supply Chain',
      label:        'Port Dwell Time — Tauranga FEU',
      ariaLabel:    'Port Dwell Time, 6 to 48 hours',
      unit:         ' h',
      defaultValue: 18,
      ticks:        ['6h', '18h avg', '36h', '48h'],
      defaultBadge: 'Within baseline'
    },
    {
      id:           'cyclone',
      category:     'Scenario',
      label:        'Cyclone Risk Index',
      ariaLabel:    'Cyclone Risk Index, 0 none to 100 Category 3 plus',
      unit:         '%',
      defaultValue: 5,
      ticks:        ['None', 'Cat 1', 'Cat 2', 'Cat 3+'],
      defaultBadge: 'No active systems'
    },
    {
      id:           'frost',
      category:     'Scenario',
      label:        'Frost Event Probability',
      ariaLabel:    'Frost Event Probability, 0 none to 100 severe',
      unit:         '%',
      defaultValue: 3,
      ticks:        ['None', 'Light', 'Moderate', 'Severe'],
      defaultBadge: 'No frost risk'
    }
  ],

  /* ── Corridor geometry (GIS canvas %) ────────────────────────── */
  corridors: {
    /**
     * Canvas-space vector paths (x/y as fraction of canvas dimensions).
     * Used by the agent-based GIS simulation.
     */
    canvas: [
      {
        id:   'katikati',
        name: 'Katikati–Tauranga',
        km:   55,
        path: [{ x: .08, y: .32 }, { x: .22, y: .38 }, { x: .38, y: .44 }, { x: .53, y: .49 }]
      },
      {
        id:   'tepuke',
        name: 'Te Puke–Tauranga',
        km:   38,
        path: [{ x: .28, y: .72 }, { x: .36, y: .62 }, { x: .44, y: .54 }, { x: .53, y: .49 }]
      },
      {
        id:   'opotiki',
        name: 'Ōpōtiki–Tauranga',
        km:   100,
        path: [{ x: .90, y: .78 }, { x: .74, y: .66 }, { x: .63, y: .57 }, { x: .53, y: .49 }]
      }
    ],

    /** Destination point shared by all canvas corridors. */
    canvasPort: { x: .53, y: .49 },

    /**
     * Real-world geographic locations for the Mapbox regional map.
     * coords: [longitude, latitude] (WGS84).
     * risk/dm/otif/vol are initial display values — overridden by live payload.
     */
    locations: [
      { id: 'katikati',  name: 'Katikati',  coords: [175.917, -37.550], risk: 12, dm: 16.34, otif: 94, vol: 28 },
      { id: 'tepuke',    name: 'Te Puke',   coords: [176.327, -37.789], risk: 18, dm: 16.20, otif: 92, vol: 35 },
      { id: 'pongakawa', name: 'Pongakawa', coords: [176.414, -37.811], risk: 22, dm: 16.27, otif: 91, vol: 22 },
      { id: 'tauranga',  name: 'Tauranga',  coords: [176.166, -37.687], risk: 28, dm: 16.16, otif: 89, vol: 18 },
      { id: 'opotiki',   name: 'Ōpōtiki',   coords: [177.286, -38.005], risk: 64, dm: 15.96, otif: 76, vol: 12 }
    ],

    /** Export gateway — rendered as a distinct port marker on the Mapbox map. */
    port: {
      name:    'Port of Tauranga',
      address: 'Sulphur Point · 66 Mirrielees Road, Tauranga 3110',
      coords:  [176.1692, -37.6705]
    },

    /**
     * Subzone-to-corridor mapping keys.
     * When the ETL payload provides subzone data, these keys resolve which
     * corridor's dry matter distribution should be enriched.
     */
    subzoneMap: {
      katikati: ['Katikati'],
      tepuke:   ['Te Puke', 'Pongakawa'],
      opotiki:  ['Opotiki']
    }
  },

  /* ── UI copy ──────────────────────────────────────────────────── */
  copy: {
    topbar: {
      brand:   'APOPHENIA',
      edition: 'Risk Intelligence',
      status:  'All systems operational',
      live:    'LIVE · NZT'
    },
    sidebar: {
      logo: 'Apophenia',
      sub:  'Bay of Plenty · 2026 SEASON',
      schema: 'Intelligence Engine · Export + Quality + Logistics'
    },
    hero: {
      eyebrowTemplate:      'CURRENT POSITION · WEEK {WK} · NZT',
      actionLabel:          'RECOMMENDED ACTION',
      defaultRecommendation:'Maintain current allocation. Recheck on Monday cycle.',
      modelVersion:         'APO v4'
    },
    packhouse: {
      modern: { pte: 100, label: 'Modern · PTE 100', readout: '100% · 6h target · −0 days' },
      legacy: { pte: 65,  label: 'Legacy · PTE 65',  readout: '65% · 12h pull-down · −4 days' }
    }
  }
});
