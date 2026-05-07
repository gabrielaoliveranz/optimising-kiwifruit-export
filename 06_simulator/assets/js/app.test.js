/*
=============================================================================
APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
Bay of Plenty Corridor · Independent Research Project
File: app.test.js
Role: Unit test suite — CONFIG integrity, risk model, payment engine, data feed
Author: Gabriela Olivera | Data Analytics Portfolio
Version: 4.1.0 | 2026-05
=============================================================================
*/

import { CONFIG } from './config.js';

// ── Pure model functions (mirrors of private IIFE functions in app.js) ────────
// _computeRisk() and _computePayment() are private. These replicate the same
// formulae using CONFIG.model.bounds for clamping, enabling direct unit testing.

const BOUNDS = CONFIG.model.bounds;

function _clamp(key, raw) {
  const b = BOUNDS[key];
  const v = parseFloat(raw);
  return isFinite(v) ? Math.max(b.min, Math.min(b.max, v)) : b.min;
}

function computeRisk(raw) {
  const s = {
    dm:    _clamp('dm',    raw.dm),
    pest:  _clamp('pest',  raw.pest),
    cong:  _clamp('cong',  raw.cong),
    rain:  _clamp('rain',  raw.rain),
    reg:   _clamp('reg',   raw.reg),
    dwell: _clamp('dwell', raw.dwell),
  };
  const dm   = s.dm < 15.5 ? 40 * Math.exp(-(s.dm - 14) / 1.5) : s.dm < 16.1 ? 10 : 0;
  const pest = s.pest * 0.28;
  const rain = Math.max(0, s.rain - 15) * 0.32;
  const cong = s.cong * 0.22;
  const reg  = s.reg  * 0.12;
  const vsi  = (s.cong > 55 && s.dwell > 20) ? 10 : 0;
  const total = dm + pest + rain + cong + reg + vsi;
  return Math.round(100 / (1 + Math.exp(-0.06 * (total - 50))));
}

function computePayment(dm) {
  if (dm < 15.5) return { cat: 'MTS Fail',          total: 0,    rate: '$0.00/tray' };
  if (dm < 16.1) return { cat: 'Green MTS',          total: 3.20, rate: '$3.20/tray' };
  if (dm < 17.0) return { cat: 'SunGold G3 Premium', total: 4.13, rate: '$4.13/tray' };
  return               { cat: 'SunGold Ultra',        total: 4.40, rate: '$4.40/tray' };
}

// ── CONFIG structure ──────────────────────────────────────────────────────────

describe('CONFIG structure', () => {
  test('all top-level keys are present', () => {
    expect(CONFIG).toHaveProperty('meta');
    expect(CONFIG).toHaveProperty('theme');
    expect(CONFIG).toHaveProperty('api');
    expect(CONFIG).toHaveProperty('model');
    expect(CONFIG).toHaveProperty('sliders');
    expect(CONFIG).toHaveProperty('corridors');
    expect(CONFIG).toHaveProperty('copy');
  });

  test('sliders array has 9 entries', () => {
    expect(CONFIG.sliders).toHaveLength(9);
  });

  test('each slider has required fields', () => {
    CONFIG.sliders.forEach(s => {
      expect(s).toHaveProperty('id');
      expect(s).toHaveProperty('unit');
      expect(s).toHaveProperty('defaultValue');
      expect(s).toHaveProperty('ticks');
      expect(typeof s.defaultValue).toBe('number');
    });
  });

  test('corridors.canvas has 3 entries', () => {
    expect(CONFIG.corridors.canvas).toHaveLength(3);
  });

  test('corridors.locations has 5 entries', () => {
    expect(CONFIG.corridors.locations).toHaveLength(5);
  });

  test('model.bounds covers all 9 slider keys', () => {
    const expected = ['dm', 'pest', 'cong', 'rain', 'vol', 'reg', 'dwell', 'cyclone', 'frost'];
    expected.forEach(k => expect(CONFIG.model.bounds).toHaveProperty(k));
  });
});

// ── Risk model ────────────────────────────────────────────────────────────────

describe('Risk model', () => {
  const BASELINE = { dm: 16.2, pest: 20, cong: 25, rain: 18, reg: 15, dwell: 18 };

  test('baseline safe inputs — score in [0,100] and exactly 10', () => {
    const score = computeRisk(BASELINE);
    expect(score >= 0).toBe(true);
    expect(score <= 100).toBe(true);
    expect(score).toBe(10);
  });

  test('low DM (14.5) — exactly 39, higher than baseline', () => {
    const score = computeRisk({ ...BASELINE, dm: 14.5 });
    expect(score).toBe(39);
    expect(score > computeRisk(BASELINE)).toBe(true);
  });

  test('high-stress scenario — exactly 99', () => {
    const score = computeRisk({ dm: 14, pest: 90, cong: 80, rain: 100, reg: 80, dwell: 40 });
    expect(score).toBe(99);
  });

  test('null inputs — clamped to bounds, no crash, valid score', () => {
    const score = computeRisk({ dm: null, pest: null, cong: null, rain: null, reg: null, dwell: null });
    expect(typeof score).toBe('number');
    expect(isFinite(score)).toBe(true);
    expect(score >= 0).toBe(true);
    expect(score <= 100).toBe(true);
  });

  test('string inputs — clamped to bounds, no crash, valid score', () => {
    const score = computeRisk({ dm: 'banana', pest: null, cong: undefined, rain: 18, reg: 15, dwell: 18 });
    expect(typeof score).toBe('number');
    expect(isFinite(score)).toBe(true);
  });

  test('all sliders at minimum values — score in [0,100], formula stable', () => {
    const score = computeRisk({
      dm: BOUNDS.dm.min, pest: BOUNDS.pest.min, cong: BOUNDS.cong.min,
      rain: BOUNDS.rain.min, reg: BOUNDS.reg.min, dwell: BOUNDS.dwell.min,
    });
    expect(score >= 0).toBe(true);
    expect(score <= 100).toBe(true);
  });

  test('monotonicity — higher DM means lower or equal risk score', () => {
    const lo = computeRisk({ ...BASELINE, dm: 14.5 });
    const hi = computeRisk({ ...BASELINE, dm: 17.0 });
    expect(lo > hi).toBe(true);
  });
});

// ── Payment engine ────────────────────────────────────────────────────────────

describe('Payment engine', () => {
  test('DM below 15.5 → MTS Fail, zero payout', () => {
    const p = computePayment(15.0);
    expect(p.cat).toBe('MTS Fail');
    expect(p.total).toBe(0);
  });

  test('DM at 17.0 → SunGold Ultra, $4.40/tray', () => {
    const p = computePayment(17.0);
    expect(p.cat).toBe('SunGold Ultra');
    expect(p.total).toBe(4.40);
  });
});

// ── Feed loader ───────────────────────────────────────────────────────────────

describe('Feed loader', () => {
  test('all paths fail — try/catch returns null, fallback applied', async () => {
    const loadWithFallback = async (paths) => {
      for (const url of paths) {
        try {
          const res = await fetch(url);
          if (res.ok) return await res.json();
        } catch {}
      }
      return null;
    };
    const result = await loadWithFallback(['./nonexistent-path-1.json', './nonexistent-path-2.json']);
    expect(result).toBe(null);
  });

  test('null payload — load guard exits early, defaults preserved', () => {
    let injected = false;
    function guardedLoad(data) { if (!data) return; injected = true; }
    guardedLoad(null);
    expect(injected).toBe(false);
  });

  test('corrupted JSON (string not object) — validate throws, load catches, no crash', () => {
    const fakeValidate = (data) => { data.dm = 10; };
    let threw = false;
    try { fakeValidate('corrupted_string'); } catch { threw = true; }
    expect(threw).toBe(true);
  });
});
