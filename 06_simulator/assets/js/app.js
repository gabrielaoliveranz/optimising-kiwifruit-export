/*
=============================================================================
APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
Bay of Plenty Corridor · Independent Research Project
File: app.js
Role: Core engine — domain-agnostic rendering, calculation, simulation,
      and event binding. All domain values are read from config.js.
      This file must never contain hardcoded business strings, geographic
      coordinates, or domain-specific constants.
Author: Gabriela Olivera | Data Analytics Portfolio
Version: 4.1.0 | 2026-05
=============================================================================
*/

import { CONFIG } from './config.js';
'use strict';
const APO = (() => {

  /* ── PRIVATE STATE ─────────────────────────────────────────── */
  const _state = {
    running: false, speed: 1, tick: 0,
    totalReturn: 0, arrivedCount: 0,
    agents: [], charts: {}, riskHistory: [],
    packhouse: 'modern',
    sliders: { dm:16.2, pest:20, cong:25, rain:18, vol:100, reg:15, dwell:18, cyclone:5, frost:3 }
  };

  /* ── INPUT BOUNDS — sourced from CONFIG (config.js) */
  const BOUNDS = CONFIG.model.bounds;
  function _clamp(key, raw) {
    const b = BOUNDS[key]; const v = parseFloat(raw);
    return isFinite(v) ? Math.max(b.min, Math.min(b.max, v)) : b.min;
  }

  /* ── CORRIDORS + PORT — sourced from CONFIG (config.js) */
  const CORRIDORS = CONFIG.corridors.canvas;
  const PORT      = CONFIG.corridors.canvasPort;

  /* ── RISK MODEL (Calibrated against public industry quality standards) ─── */
  function _computeRisk() {
    const s = _state.sliders;
    const dm      = s.dm < 15.5 ? 40 * Math.exp(-(s.dm - 14) / 1.5) : s.dm < 16.1 ? 10 : 0;
    const pest    = s.pest    * 0.28;
    const rain    = Math.max(0, s.rain - 15) * 0.32;
    const cong    = s.cong    * 0.22;
    const reg     = s.reg     * 0.12;
    const vsi     = (s.cong > 55 && s.dwell > 20) ? 10 : 0;
    const cyclone = s.cyclone * 0.10;
    const frost   = s.frost   * 0.08;
    const raw     = dm + pest + rain + cong + reg + vsi + cyclone + frost;
    return Math.round(100 / (1 + Math.exp(-0.06 * (raw - 50))));
  }

  /* ── PAYMENT ENGINE (Grower Payments Booklet 2026) ─────────── */
  function _computePayment(dm) {
    if (dm < 15.5) return { cat:'MTS Fail',          submit:0,    taste:0,    total:0,    rate:'$0.00/tray' };
    if (dm < 16.1) return { cat:'Green MTS',          submit:3.20, taste:0,    total:3.20, rate:'$3.20/tray' };
    if (dm < 17.0) return { cat:'SunGold G3 Premium', submit:3.60, taste:0.53, total:4.13, rate:'$4.13/tray' };
    return               { cat:'SunGold Ultra',        submit:3.60, taste:0.80, total:4.40, rate:'$4.40/tray' };
  }

  /* ── AGENT FACTORY ──────────────────────────────────────────── */
  let _agentId = 0;
  let _subzoneData = null;
  function _spawnAgent() {
    const cor  = CORRIDORS[Math.floor(Math.random() * 3)];
    const base = (cor._szDm  !== undefined) ? cor._szDm  : _state.sliders.dm;
    const std  = (cor._szStd !== undefined) ? cor._szStd : 0.7;
    const dm   = _clamp('dm', base + (Math.random() - 0.5) * std * 2);
    return {
      id: `EXP-2026-${++_agentId}`, corridor: cor, progress: 0,
      dm: parseFloat(dm.toFixed(2)), trays: 800 + Math.floor(Math.random() * 600),
      mts_pass: dm >= 15.5, vsi_score: 0, risk_score: _computeRisk()
    };
  }

  function _setSubzones(sz) {
    _subzoneData = sz;
    CORRIDORS.forEach(cor => {
      if (cor.id === 'katikati' && sz['Katikati']) {
        cor._szDm = sz['Katikati'].dm_mean; cor._szStd = sz['Katikati'].dm_std || 0.7;
      } else if (cor.id === 'tepuke') {
        const tp = sz['Te Puke'], pk = sz['Pongakawa'];
        if (tp && pk) { cor._szDm = (tp.dm_mean + pk.dm_mean) / 2; cor._szStd = ((tp.dm_std || 0.7) + (pk.dm_std || 0.7)) / 2; }
        else if (tp)  { cor._szDm = tp.dm_mean; cor._szStd = tp.dm_std || 0.7; }
      } else if (cor.id === 'opotiki' && sz['Opotiki']) {
        cor._szDm = sz['Opotiki'].dm_mean; cor._szStd = sz['Opotiki'].dm_std || 0.7;
      }
    });
  }

  /* ── GIS CANVAS ─────────────────────────────────────────────── */
  let _ctx = null, _cv = null, _W = 0, _H = 0, _hovered = null;

  function _initGis() {
    _cv = document.getElementById('gisCanvas');
    if (!_cv) return;
    _ctx = _cv.getContext('2d');
    _resizeGis();
    new ResizeObserver(_resizeGis).observe(_cv.parentElement);
    _cv.addEventListener('mousemove', e => { const r=_cv.getBoundingClientRect(); _hover(e.clientX-r.left, e.clientY-r.top); });
    _cv.addEventListener('touchstart', e => { if(!e.touches[0]) return; const r=_cv.getBoundingClientRect(); _hover(e.touches[0].clientX-r.left,e.touches[0].clientY-r.top); },{passive:true});
    _cv.addEventListener('click', e => { if(_hovered){const r=_cv.getBoundingClientRect();_showPopup(_hovered,e.clientX-r.left,e.clientY-r.top);} });
  }
  function _resizeGis() {
    if (!_cv) return;
    const rect = _cv.parentElement.getBoundingClientRect();
    _cv.width = _W = rect.width; _cv.height = _H = Math.max(rect.height, 360);
    _drawGis();
  }
  function _lerp(a,b,t){ return a+(b-a)*t; }
  function _agentXY(ag) {
    const p=ag.corridor.path, t=ag.progress;
    const seg=Math.min(Math.floor(t*(p.length-1)),p.length-2);
    const st=(t*(p.length-1))-seg;
    return { x:_lerp(p[seg].x,p[seg+1].x,st)*_W, y:_lerp(p[seg].y,p[seg+1].y,st)*_H };
  }

  function _drawGis() {
    if (!_ctx) return;
    const c = _ctx;
    c.clearRect(0,0,_W,_H);
    /* background */
    c.fillStyle='#edf2ed'; c.fillRect(0,0,_W,_H);
    /* subtle grid */
    c.strokeStyle='rgba(0,99,56,.05)'; c.lineWidth=1;
    for(let i=1;i<8;i++){c.beginPath();c.moveTo(_W/8*i,0);c.lineTo(_W/8*i,_H);c.stroke();}
    for(let j=1;j<6;j++){c.beginPath();c.moveTo(0,_H/6*j);c.lineTo(_W,_H/6*j);c.stroke();}
    /* corridor paths — warm gray dashed */
    CORRIDORS.forEach(cor=>{
      c.beginPath(); c.strokeStyle='#ebeae5'; c.lineWidth=1.5; c.setLineDash([6,4]);
      cor.path.forEach((p,i)=>{ i===0?c.moveTo(p.x*_W,p.y*_H):c.lineTo(p.x*_W,p.y*_H); });
      c.stroke(); c.setLineDash([]);
      const s=cor.path[0]; const label=cor.id==='katikati'?'Katikati':cor.id==='tepuke'?'Te Puke':'Ōpōtiki';
      c.fillStyle='rgba(26,32,48,.65)'; c.font=`bold ${Math.max(10,_W*0.016)}px system-ui,sans-serif`;
      c.fillText(label, s.x*_W+(cor.id==='opotiki'?-70:6), s.y*_H-6);
    });
    /* port — brand ink */
    const px=PORT.x*_W, py=PORT.y*_H;
    const portR=Math.max(11,_W*0.018);
    c.beginPath(); c.fillStyle='#0a1410'; c.arc(px,py,portR,0,Math.PI*2); c.fill();
    c.fillStyle='#fff'; c.font=`bold ${Math.max(8,_W*0.012)}px system-ui,sans-serif`; c.textAlign='center';
    c.fillText('PORT',px,py+4); c.textAlign='left';
    c.fillStyle='rgba(26,32,48,.7)'; c.font=`${Math.max(10,_W*0.015)}px system-ui,sans-serif`;
    c.fillText('Tauranga',px+portR+3,py+5);
    /* agents — risk-coded color, tray-proportional size */
    _state.agents.forEach(ag=>{
      const pos=_agentXY(ag); const isHov=_hovered&&_hovered.id===ag.id;
      const isPrem=ag.dm>17;
      const nodeColor=ag.risk_score>=60?'#a13030':ag.risk_score>=25?'#c97a2c':'#00674a';
      const baseR=Math.max(3, 4+(ag.trays-800)/600*3);   /* 4–7px proportional to trays 800–1400 */
      const r=isHov?baseR+3:baseR;
      if(isPrem){
        const g=c.createRadialGradient(pos.x,pos.y,0,pos.x,pos.y,r+10);
        g.addColorStop(0,'rgba(250,204,21,.50)'); g.addColorStop(1,'rgba(250,204,21,0)');
        c.beginPath(); c.arc(pos.x,pos.y,r+10,0,Math.PI*2); c.fillStyle=g; c.fill();
      }
      c.beginPath(); c.arc(pos.x,pos.y,r,0,Math.PI*2);
      c.fillStyle=nodeColor; c.fill();
      c.strokeStyle='rgba(255,255,255,.9)'; c.lineWidth=1.5; c.stroke();
    });
  }

  function _hover(mx,my) {
    let found=null;
    _state.agents.forEach(ag=>{ const p=_agentXY(ag); if(Math.hypot(p.x-mx,p.y-my)<16) found=ag; });
    _hovered=found; _drawGis();
    found ? _showPopup(found, _agentXY(found).x, _agentXY(found).y) : _hidePopup();
  }
  function _showPopup(ag,px,py) {
    const pop=document.getElementById('gisPopup'); if(!pop) return;
    const pay=_computePayment(ag.dm);
    _setText('popup_id',   ag.id);
    _setText('popup_dm',   ag.dm.toFixed(1)+'%');
    _setText('popup_zone', ag.corridor.name);
    _setText('popup_risk', ag.risk_score);
    _setText('popup_return', pay.rate);
    _setText('popup_status', ag.progress>=1?'Arrived · Port of Tauranga':`In transit · ${Math.round(ag.progress*100)}% · ${ag.corridor.name}`);
    const mts=document.getElementById('popup_mts');
    if(mts){mts.textContent=ag.mts_pass?'MTS PASS':'MTS FAIL'; mts.className='popup-mts '+(ag.mts_pass?'pass':'fail');}
    _setText('popup_insight', _insight(ag));
    let left=px+14, top=py-65;
    if(left+215>_W) left=px-225; if(top<0) top=py+14;
    pop.style.left=left+'px'; pop.style.top=top+'px'; pop.classList.add('show');
  }
  function _hidePopup(){ const p=document.getElementById('gisPopup'); if(p) p.classList.remove('show'); }
  function _insight(ag) {
    if(!ag.mts_pass) return 'CRITICAL: DM below 15.5% MTS threshold. Market access blocked. Do not submit.';
    if(ag.risk_score>60) return `High risk (${ag.risk_score}/100). Rainfall and congestion compounding delay. Prioritise cold storage upon arrival.`;
    if(ag.risk_score>25) return `Moderate risk (${ag.risk_score}/100). SH2 congestion at ${_state.sliders.cong}% — monitor DM erosion in transit.`;
    if(ag.dm>17) return `Premium tier confirmed. DM ${ag.dm}% qualifies SunGold Ultra at ${_computePayment(ag.dm).rate}. Premium uplift active.`;
    return `Stable conditions. Premium tier payments accruing at ${_computePayment(ag.dm).rate}. No corrective action required.`;
  }

  /* ── MAPBOX REGIONAL MAP ────────────────────────────────────── */
  const _APO_LOCATIONS = CONFIG.corridors.locations;
  const _APO_PORT      = CONFIG.corridors.port;

  function _riskColor(r) {
    if (r < 25) return '#00674a';
    if (r < 60) return '#c97a2c';
    return '#a13030';
  }

  let _apoMap = null;
  let _apoMapInited = false;

  function _initMapbox() {
    const fb = document.getElementById('apoMapFallback');

    if (_apoMapInited && _apoMap) {
      if (fb) fb.hidden = true;
      return _apoMap;
    }

    if (fb) fb.hidden = true;

    const token = window.APO_CONFIG?.mapboxToken;
    if (!token || token.includes('PLACEHOLDER') || token.includes('YOUR_')) return _showMapFallback('Map service is not configured.');
    if (!/^pk\.[\w-]+\.[\w-]+$/.test(token)) return _showMapFallback('Map credentials are invalid.');
    if (typeof mapboxgl === 'undefined') return _showMapFallback('Map library could not be loaded.');

    try {
      mapboxgl.accessToken = token;
      _apoMap = new mapboxgl.Map({
        container: 'apoMap',
        style: CONFIG.api.mapbox.style,
        center: CONFIG.api.mapbox.center,
        zoom: CONFIG.api.mapbox.zoom,
        attributionControl: { compact: true }
      });

      let loaded = false;
      let errored = false;

      _apoMap.on('load', () => {
        loaded = true;
        if (fb) fb.hidden = true;
        _addMapLayers();
        if (_nztaRefreshTimer) { clearInterval(_nztaRefreshTimer); _nztaRefreshTimer = null; }
      });

      _apoMap.on('error', (e) => {
        const msg = (e?.error?.message || '').toLowerCase();
        if (msg.includes('401') || msg.includes('403') || msg.includes('unauthorized')) {
          errored = true;
          _showMapFallback('Map authorization failed.');
        } else if (msg.includes('quota') || msg.includes('429')) {
          errored = true;
          _showMapFallback('Map service quota exceeded.');
        }
      });

      setTimeout(() => {
        if (!loaded && !errored) _showMapFallback('Map is taking too long to load.');
      }, 12000);

      _apoMap.addControl(new mapboxgl.NavigationControl({ showCompass: false }), 'top-right');
      _apoMapInited = true;
      return _apoMap;
    } catch (err) {
      console.warn('[Mapbox] init failed:', err);
      _showMapFallback('Map could not be initialized.');
      return null;
    }
  }

  function _showMapFallback(msg) {
    const fb = document.getElementById('apoMapFallback');
    if (!fb) return;
    const t = fb.querySelector('.map-fallback-text');
    if (t && msg) t.textContent = msg + ' Operational data and risk metrics remain fully functional.';
    fb.hidden = false;
  }

  function _addMapLayers() {
    if (!_apoMap) return;
    const lines = _APO_LOCATIONS.map(loc => ({
      type: 'Feature',
      properties: { name: loc.name, risk: loc.risk },
      geometry: { type: 'LineString', coordinates: [loc.coords, _APO_PORT.coords] }
    }));
    _apoMap.addSource('corridor-lines', { type: 'geojson', data: { type: 'FeatureCollection', features: lines } });
    _apoMap.addSource('mapbox-traffic', {
      type: 'vector',
      url: CONFIG.api.mapbox.trafficUrl
    });
    _apoMap.addLayer({
      id: 'traffic-layer',
      type: 'line',
      source: 'mapbox-traffic',
      'source-layer': 'traffic',
      minzoom: 6,
      paint: {
        'line-width': ['interpolate', ['linear'], ['zoom'], 6, 1.5, 14, 4],
        'line-color': ['match', ['get', 'congestion'], 'low', '#00674a', 'moderate', '#c9a961', 'heavy', '#c97a2c', 'severe', '#a13030', 'rgba(0,0,0,0)'],
        'line-opacity': 0.8
      }
    });
    _apoMap.addLayer({
      id: 'corridor-line-layer',
      type: 'line',
      source: 'corridor-lines',
      paint: {
        'line-color': ['case', ['<', ['get', 'risk'], 25], '#00674a', ['<', ['get', 'risk'], 60], '#c97a2c', '#a13030'],
        'line-width': 2,
        'line-opacity': 0.6,
        'line-dasharray': [2, 2]
      }
    });
    const badge = document.getElementById('nztaStatusBadge');
    if (badge) {
      badge.innerHTML = `<span class="dot dot-live"></span> Traffic Live · Mapbox`;
      badge.dataset.state = 'live';
    }

    _APO_LOCATIONS.forEach(loc => {
      const el = document.createElement('div');
      el.style.cssText = `width:${14+loc.vol*0.4}px;height:${14+loc.vol*0.4}px;background:${_riskColor(loc.risk)};border:3px solid #fff;border-radius:50%;box-shadow:0 2px 8px rgba(0,0,0,0.25);cursor:pointer;`;
      const popup = new mapboxgl.Popup({ offset: 18, closeButton: false }).setHTML(`
        <h4>${loc.name}</h4>
        <div class="pop-row">Risk score: <strong>${loc.risk}/100</strong></div>
        <div class="pop-row">Dry matter: <strong>${loc.dm}%</strong></div>
        <div class="pop-row">OTIF: <strong>${loc.otif}%</strong></div>
        <div class="pop-row">Volume: <strong>${loc.vol}M trays</strong></div>
      `);
      new mapboxgl.Marker(el).setLngLat(loc.coords).setPopup(popup).addTo(_apoMap);
    });

    const portEl = document.createElement('div');
    portEl.style.cssText = `width:28px;height:28px;background:#0a1410;border:3px solid #fff;border-radius:50%;box-shadow:0 2px 10px rgba(0,0,0,0.3);display:flex;align-items:center;justify-content:center;color:#fff;font-size:9px;font-weight:700;font-family:'Inter',sans-serif;`;
    portEl.textContent = 'PORT';
    new mapboxgl.Marker(portEl).setLngLat(_APO_PORT.coords).setPopup(new mapboxgl.Popup({ offset: 18, closeButton: false }).setHTML(`<h4>${_APO_PORT.name}</h4><div class="pop-row">${_APO_PORT.address}</div><div class="pop-row">Main export gateway · Bay of Plenty</div>`)).addTo(_apoMap);
  }

  /* ── NZTA LIVE TRAFFIC ──────────────────────────────────────── */
  const _NZTA_ENDPOINTS = [
    'https://www.journeys.nzta.govt.nz/assets/journey-planner-tools/cars/data.json',
    'https://api.gisngis.com/nzta/sh2/events.json'
  ];
  let _nztaRefreshTimer = null;
  let _nztaLastFetch = null;

  async function _fetchNZTAEvents() {
    for (const url of _NZTA_ENDPOINTS) {
      try {
        const res = await fetch(url, { mode: 'cors', cache: 'no-store' });
        if (!res.ok) continue;
        const data = await res.json();
        const events = _parseNZTAResponse(data);
        if (events && events.length >= 0) {
          _nztaLastFetch = new Date();
          return events;
        }
      } catch (err) {
        console.warn('[NZTA] fetch failed:', url, err.message);
      }
    }
    return null;
  }

  function _parseNZTAResponse(data) {
    const arr = data?.features || data?.events || data?.results || data?.data || [];
    if (!Array.isArray(arr)) return [];
    return arr
      .filter(e => {
        const road = (e?.properties?.road || e?.road || e?.RoadName || '').toString().toUpperCase();
        const region = (e?.properties?.region || e?.region || '').toString().toUpperCase();
        return road.includes('SH2') || region.includes('BAY OF PLENTY') || region.includes('BOP');
      })
      .map(e => {
        const p = e.properties || e;
        const coords = e.geometry?.coordinates || (p.lon && p.lat ? [p.lon, p.lat] : null);
        if (!coords) return null;
        const flat = Array.isArray(coords[0]) ? coords[0] : coords;
        return {
          coords: flat,
          type: (p.eventType || p.type || 'Event').toString(),
          severity: (p.severity || p.impact || 'unknown').toString().toLowerCase(),
          description: p.description || p.title || p.summary || 'Traffic event',
          startTime: p.startDate || p.startTime || null
        };
      })
      .filter(Boolean);
  }

  function _renderNZTAEvents(events) {
    if (!_apoMap || !_apoMap.isStyleLoaded()) return;
    if (_apoMap.getLayer('nzta-events-layer')) _apoMap.removeLayer('nzta-events-layer');
    if (_apoMap.getSource('nzta-events')) _apoMap.removeSource('nzta-events');
    if (!events || !events.length) return;
    const features = events.map(ev => ({
      type: 'Feature',
      properties: { type: ev.type, severity: ev.severity, description: ev.description },
      geometry: { type: 'Point', coordinates: ev.coords }
    }));
    _apoMap.addSource('nzta-events', { type: 'geojson', data: { type: 'FeatureCollection', features } });
    _apoMap.addLayer({
      id: 'nzta-events-layer',
      type: 'circle',
      source: 'nzta-events',
      paint: {
        'circle-radius': 8,
        'circle-color': ['match', ['get', 'severity'], 'high', '#a13030', 'medium', '#c97a2c', 'low', '#c9a961', '#4a5550'],
        'circle-stroke-color': '#ffffff',
        'circle-stroke-width': 2,
        'circle-opacity': 0.85
      }
    });
    _apoMap.on('click', 'nzta-events-layer', (e) => {
      const f = e.features[0]; if (!f) return;
      new mapboxgl.Popup({ offset: 12, closeButton: true })
        .setLngLat(f.geometry.coordinates)
        .setHTML(`<h4>NZTA · ${f.properties.type}</h4><div class="pop-row"><strong>Severity:</strong> ${f.properties.severity}</div><div class="pop-row">${f.properties.description}</div>`)
        .addTo(_apoMap);
    });
    _apoMap.on('mouseenter', 'nzta-events-layer', () => { _apoMap.getCanvas().style.cursor = 'pointer'; });
    _apoMap.on('mouseleave', 'nzta-events-layer', () => { _apoMap.getCanvas().style.cursor = ''; });
  }

  async function _refreshNZTA() {
    const events = await _fetchNZTAEvents();
    if (events !== null) {
      _renderNZTAEvents(events);
      _updateNZTABadge('live', events.length);
    } else {
      _updateNZTABadge('unavailable', 0);
    }
  }

  function _updateNZTABadge(state, count) {
    const badge = document.getElementById('nztaStatusBadge');
    if (!badge) return;
    if (state === 'live') {
      badge.innerHTML = `<span class="dot dot-live"></span> NZTA Live · ${count} events`;
      badge.dataset.state = 'live';
    } else {
      badge.innerHTML = `<span class="dot dot-off"></span> NZTA · offline`;
      badge.dataset.state = 'off';
    }
  }

  function _startNZTARefresh() {
    _refreshNZTA();
    if (_nztaRefreshTimer) clearInterval(_nztaRefreshTimer);
    _nztaRefreshTimer = setInterval(_refreshNZTA, 300000);
  }

  /* ── WHEN CARD SPARKLINE ────────────────────────────────────── */
  function _updateHeroSparkline() {
    const svg = document.getElementById('heroTimeSvg'); if (!svg) return;
    const hist = _state.riskHistory;
    if (hist.length < 2) return;
    const W = 200, H = 60, pad = 4;
    const min = 0, max = 100;
    const tx = (i, n) => pad + (i / (n - 1)) * (W - pad * 2);
    const ty = v => H - pad - ((v - min) / (max - min)) * (H - pad * 2);
    const pts = hist.map((v, i) => `${tx(i, hist.length)},${ty(v)}`).join(' ');
    const peakIdx = hist.indexOf(Math.max(...hist));
    const peakX = tx(peakIdx, hist.length);
    const peakY = ty(hist[peakIdx]);
    const peakWk = _state.tick > 0 ? Math.min(26, _state.tick - hist.length + peakIdx + 1) : 17;
    svg.innerHTML = `
      <polyline points="${pts}" fill="none" stroke="var(--gold-deep)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
      <line x1="${peakX}" y1="${pad}" x2="${peakX}" y2="${H}" stroke="var(--risk-mid)" stroke-width="1" stroke-dasharray="3,2" opacity="0.7"/>
      <circle cx="${peakX}" cy="${peakY}" r="3.5" fill="var(--risk-mid)"/>`;
    const lbl = document.querySelector('.when-peak-label');
    if (lbl) lbl.textContent = `Wk${peakWk} — peak risk · ${Math.round(hist[peakIdx])}/100`;
    _setText('triad-when-wk', `Wk${peakWk}`);
  }

  /* ── SIMULATION TICK ────────────────────────────────────────── */
  let _interval=null;
  function _tick() {
    const s=_state.sliders;
    const cN=s.cong/100, rN=Math.min(1,s.rain/60);
    if(_state.agents.length<14 && Math.random()<0.38) _state.agents.push(_spawnAgent());
    const done=[];
    _state.agents.forEach(ag=>{
      const base=0.042/(ag.corridor.km/38);
      ag.progress += base*(1-cN*0.55)*(1-rN*0.25)*_state.speed;
      ag.vsi_score  = Math.min(100, ag.vsi_score + cN*3.8 + rN*1.3);
      if(ag.progress>=1){ const pay=_computePayment(ag.dm); _state.totalReturn+=pay.total*ag.trays; _state.arrivedCount++; done.push(ag.id); }
    });
    _state.agents=_state.agents.filter(a=>!done.includes(a.id));
    _state.riskHistory.push(_computeRisk());
    if(_state.riskHistory.length>26) _state.riskHistory.shift();
    _state.tick++;
    _updateDOM(); _drawGis(); _updateCharts();
  }

  /* ── DOM UPDATER ────────────────────────────────────────────── */
  function _updateDOM() {
    const s=_state.sliders, risk=_computeRisk();
    /* Risk gauge */
    _setText('riskScore', risk);
    const sc=document.getElementById('riskScore');
    if(sc) sc.className='rg-score'+(risk>60?' critical':risk>25?' warn':'');
    _setText('riskVerdict', risk>60?'Critical risk. Immediate corrective action required. MTS breach imminent.':risk>25?'Elevated risk. Monitor SH2 congestion and DM levels closely.':'Low risk environment. Export operations within normal parameters. Premium tier payments accruing.');
    _setText('arcLabel', risk>60?'CRITICAL':risk>25?'ELEVATED':'LOW RISK');
    _drawArcGauge(risk);
    _updateHeroSparkline();
    /* KPIs */
    const otif=Math.max(40,Math.round(100-(s.cong*0.35)-(s.rain>40?(s.rain-40)*0.2:0)));
    const ret=Math.round(384*(s.dm>=16.1?1.08:0.92)*(s.vol/100));
    const fv=((s.cong*0.08)+(s.rain>30?(s.rain-30)*0.03:0)).toFixed(1);
    const mg=(-(risk*0.06)).toFixed(1);
    _setVC('kv_otif',  otif+'%',        otif>=90?'green':otif>=75?'warn':'crit');
    _setVC('kv_returns','$'+ret+'M',     ret>=370?'green':'warn');
    _setVC('kv_freight','+'+fv+'%',     +fv<5?'green':'warn');
    _setVC('kv_margin', mg+'%',         Math.abs(+mg)<3?'green':Math.abs(+mg)<8?'warn':'crit');
    const _kd=(id,up,txt)=>{const e=document.getElementById(id);if(e)e.innerHTML=(up?'<span class="up">▲</span>':'<span class="down">▼</span>')+' '+txt;};
    _kd('kd_otif',   otif>=90,          'On-Time In-Full · SH2 + Rainfall');
    _kd('kd_returns', ret>=370,          'Seasonal estimate · premium-adjusted');
    _kd('kd_freight', +fv<5,             'vs baseline · SH2 congestion driver');
    _kd('kd_margin',  Math.abs(+mg)<3,   'Net impact across DM + Logistics');
    /* Banners */
    _toggle('mtsBanner','show', s.dm<15.5);
    _toggle('tzgBanner','show', s.dm>=16.1);
    _toggle('vsiBanner','show', s.cong>60);
    /* Economic */
    const exH=Math.max(0,s.dwell-12), cod=Math.round(312*exH+7.5*exH);
    const shelf=((exH*0.4/12)+(_state.packhouse==='legacy'?2:0)).toFixed(1);
    _setText('ev_dwell', s.dwell.toFixed(1));
    _setVC('ev_cod',   '$'+cod.toLocaleString(), cod>2000?'warn':'gold');
    _setText('ev_shelf', shelf);
    _setText('ev_reefer','$'+Math.round(7.5*exH));
    _setText('ev_pte_mode',_state.packhouse==='modern'?'Modern':'Legacy');
    _setText('ev_pte_val','PTE '+(_state.packhouse==='modern'?100:65)+' · −'+(_state.packhouse==='legacy'?4:0)+' days');
    _setVC('ev_season','$'+((cod+Math.round(7.5*exH))*2341/1e6).toFixed(1)+'M','warn');
    _setText('cod_pct_label', exH+'h excess');
    const cf=document.getElementById('codFill'); if(cf) cf.style.width=Math.min(100,Math.round(exH/36*100))+'%';
    /* PTE strip */
    const strip=document.getElementById('pteStrip');
    if(strip){ strip.innerHTML=''; const lost=parseFloat(shelf); for(let i=0;i<90;i++){const d=document.createElement('div'); d.className='pte-day'+(i>=90-Math.round(lost)?' lost':''); strip.appendChild(d);} }
    /* Grower payments — seasonal value = volume (M trays) × effective rate ($/tray) = $M NZD */
    const pay=_computePayment(s.dm);
    _setText('gp_category', pay.cat==='MTS Fail'?'MTS Failure — No Payment':pay.cat==='Green MTS'?'Green MTS Compliant':pay.cat==='SunGold G3 Premium'?'SunGold G3 Premium':'SunGold Ultra Premium');
    _setText('gp_submit', '$'+pay.submit.toFixed(2)+'/tray');
    _setText('gp_taste',  '+$'+pay.taste.toFixed(2)+'/tray');
    _setText('gp_total',  '$'+pay.total.toFixed(2)+'/tray');
    // s.vol is M trays (default 100). Guard against NaN/undefined.
    const volM = (Number.isFinite(s.vol) && s.vol > 0) ? s.vol : 100;
    const rate = (Number.isFinite(pay.total) && pay.total > 0) ? pay.total : 0;
    const maxRate = 4.40;
    const sR = Math.round(rate * volM);
    const mR = Math.round(maxRate * volM);
    _setText('gp_season', '$'+sR+'M NZD seasonal');
    _setText('gp_loss',   '−$'+Math.max(0, mR - sR)+'M vs maximum premium');
    _setText('gp_dm_note',`DM ${s.dm}% ${s.dm>=16.369?'≥':'<'} 16.369% (Ōpōtiki mean) → ${pay.cat} @ ${pay.rate}.`);
    /* VSI */
    const avgVsi=_state.agents.length?_state.agents.reduce((a,b)=>a+b.vsi_score,0)/_state.agents.length:0;
    const vLv=avgVsi>65?'critical':avgVsi>45?'high':avgVsi>25?'moderate':'safe';
    const vb=document.getElementById('vsiValBadge'); if(vb){vb.textContent=avgVsi.toFixed(1);vb.className='vsi-val-badge '+vLv;}
    const vm=document.getElementById('vsiMeterFill'); if(vm){vm.style.width=avgVsi+'%';vm.className='vsi-meter-fill '+vLv;}
    /* Corridors */
    const cl=document.getElementById('corridorList');
    if(cl) cl.innerHTML=CORRIDORS.map(c=>{
      const o=Math.round(otif*(c.km<50?1.03:c.km>80?0.91:0.98)); const col=o>=90?'green':o>=75?'warn':'crit';
      return `<div class="corridor-item"><div class="corr-name">${c.name.split('–')[0]}</div><div class="corr-track"><div class="corr-fill ${col}" style="width:${o}%"></div></div><div class="corr-val">${o}%</div></div>`;
    }).join('');
    /* Quality meter */
    const tzg=Math.min(1,Math.max(0,(s.dm-15.5)/(17.5-15.5)));
    _setText('tzgCurrentVal', tzg.toFixed(2)); _setText('tzgScore', tzg.toFixed(2));
    const tf=document.getElementById('tzgBarFill'); if(tf) tf.style.width=(tzg*100)+'%';
    _setText('tzgDmGrade', s.dm>=17?'A':s.dm>=16.1?'B':s.dm>=15.5?'C':'F');
    _setText('tzgBonus',   '$'+pay.taste.toFixed(2)); _setText('tzgPoolRate','$'+(pay.submit+pay.taste*0.6).toFixed(2));
    /* GIS stat bar */
    _setText('gs_active',  _state.agents.length);
    _setText('gs_premium', _state.agents.filter(a=>a.dm>17).length);
    _setText('gs_arrived', _state.arrivedCount);
    _setText('gs_return',  '$'+(_state.totalReturn/1e6).toFixed(1)+'M');
    _setText('simAgentCount', _state.agents.length);
    /* Analytics KPI strip */
    _setVC('bi_kpi_exportval','$'+Math.round(sR)+'M','green');
    _setVC('bi_kpi_cod','$'+cod.toLocaleString(), cod>2000?'warn':'gold');
    _setVC('bi_kpi_risk', risk, risk>60?'crit':risk>25?'warn':'green');
    _setVC('bi_kpi_otif', otif+'%', otif>=90?'green':'warn');
    /* Slider fill bars */
    Object.keys(s).forEach(k=>{
      const fi=document.getElementById('sf_'+k); if(!fi) return;
      const b=BOUNDS[k]; const pct=((s[k]-b.min)/(b.max-b.min))*100; fi.style.width=pct+'%';
      const crit=(k==='dm'&&s[k]<15.5)||(k!=='dm'&&k!=='vol'&&s[k]>75);
      fi.className='si-fill'+(crit?' critical':(!['dm','vol'].includes(k)&&s[k]>60)?' gold':'');
      /* update aria-valuenow */
      const inp=document.getElementById('s_'+k); if(inp) inp.setAttribute('aria-valuenow',s[k]);
    });
    /* System status dot */
    const dot=document.getElementById('sysStatusDot'), txt=document.getElementById('sysStatusText');
    if(dot&&txt){
      if(risk>60){dot.className='status-dot crit';txt.textContent='Critical — intervention required';}
      else if(risk>25){dot.className='status-dot warn';txt.textContent='Elevated risk — monitoring active';}
      else{dot.className='status-dot';txt.textContent='All systems operational';}
    }
    _renderExecHero();
  }

  /* ── EXECUTIVE HERO RENDERER ────────────────────────────────── */
  function _renderExecHero() {
    const s=_state.sliders, risk=_computeRisk();
    /* Severity */
    const severity=risk<30?'Stable':risk<60?'Elevated':'Critical';
    const sevEl=document.getElementById('hero-severity');
    if(sevEl){sevEl.textContent=severity;sevEl.dataset.level=severity.toLowerCase();}
    /* At-risk corridor — weighted by km + congestion + rain penalty */
    const score=c=>c.km*(1+s.cong/100*0.4+(s.rain>40?(s.rain-40)/120*0.2:0));
    const corr=CORRIDORS.slice().sort((a,b)=>score(b)-score(a))[0];
    const corrName=corr?corr.name.split('–')[0]:'Ōpōtiki';
    /* Peak week from riskHistory */
    const hist=_state.riskHistory.length>1?_state.riskHistory:[risk];
    const peakIdx=hist.indexOf(Math.max(...hist));
    const wkEl=document.getElementById('currentWeek');
    const baseWk=wkEl?parseInt(wkEl.textContent.replace(/\D/g,''))||17:17;
    const peakWk='Wk'+(baseWk+Math.max(0,peakIdx-hist.length+4));
    /* Cost exposure */
    const exH=Math.max(0,s.dwell-12), cod=Math.round(319.5*exH);
    const exposure=((cod*2341/1e6)*(s.vol/100)).toFixed(1);
    /* Recommendation — deterministic rule lookup */
    let rec;
    if(s.rain>60)      rec=`Hold dispatch from ${corrName} 48h; reroute via Tauranga inland.`;
    else if(s.cong>70) rec=`Switch ${corrName} volume to Te Puke packhouse for next 5 days.`;
    else if(s.dm<16.1) rec=`Defer ${corrName} pick window 72h; DM below taste threshold.`;
    else if(s.vol>90)  rec=`Stagger ${corrName} flow — capacity ceiling within 2 weeks.`;
    else               rec='Maintain current allocation. Recheck on Monday cycle.';
    /* Set DOM */
    _setText('hero-wk',       wkEl?wkEl.textContent.replace(/\D/g,''):'17');
    _setText('hero-corridor', corrName);
    _setText('hero-amount',   '$'+exposure+'M');
    _setText('hero-peakwk',   peakWk);
    _setText('hero-recommendation', rec);
    _setText('hero-updated',  new Date().toLocaleTimeString('en-NZ',{hour:'2-digit',minute:'2-digit'}));
    /* Triad sync */
    _setText('triad-where-name', corr?corr.name:'Ōpōtiki–Tauranga');
    _setText('triad-where-risk', risk);
    _setText('triad-when-wk',   peakWk);
    _setText('triad-cost-val',  '$'+exposure+'M');
    const costDelta=document.getElementById('triad-cost-delta');
    if(costDelta){costDelta.textContent=parseFloat(exposure)>0?'▼':'—';costDelta.className=parseFloat(exposure)>1?'down':'up';}
    /* Mini timeline */
    const tsvg=document.getElementById('heroTimeSvg');
    if(tsvg){
      const W=200,H=60,data=hist.length>3?hist:[...Array(8-Math.min(hist.length,8)).fill(hist[0]||risk),...hist].slice(-8);
      const mn=Math.min(...data),mx=Math.max(...data,mn+1);
      const tx=i=>(i/(data.length-1))*W, ty=v=>H-4-((v-mn)/(mx-mn))*(H-8);
      const pts=data.map((v,i)=>`${tx(i).toFixed(1)},${ty(v).toFixed(1)}`).join(' ');
      const pi=data.indexOf(Math.max(...data));
      tsvg.innerHTML=`<polyline points="${pts}" fill="none" stroke="var(--green-main)" stroke-width="1.5" stroke-linejoin="round"/><circle cx="${tx(pi).toFixed(1)}" cy="${ty(data[pi]).toFixed(1)}" r="3" fill="var(--risk-mid)"/>`;
    }
    /* WHERE corridor highlight */
    ['katikati','tepuke','opotiki'].forEach(cid=>{
      const el=document.getElementById('hc-'+cid); if(!el) return;
      const active=corr&&corr.id===cid;
      el.setAttribute('stroke',active?'var(--risk-mid)':'var(--green-hair)');
      el.setAttribute('stroke-width',active?'2.5':'1.5');
    });
  }

  /* ── ARC GAUGE SVG ──────────────────────────────────────────── */
  function _drawArcGauge(risk) {
    const svg = document.getElementById('arcGauge'); if (!svg) return;
    const cx = 80, cy = 88, R = 70;
    const r = Math.max(0, Math.min(100, Number(risk) || 0));
    const col = r > 60 ? '#a13030' : r > 25 ? '#c97a2c' : '#006338';
    // Map risk to angle: risk=0 → θ=π (left), risk=100 → θ=0 (right). Top semicircle.
    // SVG y is inverted, so use cy - R*sin(θ) to put arc ABOVE the baseline.
    const theta = Math.PI * (1 - r / 100);
    const pt = (t) => [cx + R * Math.cos(t), cy - R * Math.sin(t)];
    const [sx, sy] = pt(Math.PI);
    const [bx, by] = pt(0);
    const [ex, ey] = pt(theta);
    // Sweep flag = 1 (clockwise in screen coords) traces left → top → right.
    const bgPath     = `M ${sx} ${sy} A ${R} ${R} 0 0 1 ${bx} ${by}`;
    const activePath = r > 0
      ? `<path d="M ${sx} ${sy} A ${R} ${R} 0 0 1 ${ex} ${ey}" fill="none" stroke="${col}" stroke-width="8" stroke-linecap="round"/>`
      : '';
    svg.innerHTML =
      `<path d="${bgPath}" fill="none" stroke="#e2e8e2" stroke-width="8" stroke-linecap="round"/>` +
      activePath +
      `<circle cx="${ex}" cy="${ey}" r="5" fill="${col}"/>`;
  }

  /* ── PROJECTION SVG (26-week arc — accepts real ETL data or falls back to defaults) */
  function _drawProjectionSvg(arcData) {
    const svg=document.getElementById('projSvg'); if(!svg) return;
    const W=svg.clientWidth||400, H=110;
    const data=arcData||[18,20,22,28,32,30,26,24,22,26,30,35,38,32,28,24,22,20,24,28,26,22,20,18,16,14];
    const upper=data.map(v=>Math.min(100,v+12)), lower=data.map(v=>Math.max(0,v-12));
    const tx=i=>i/(data.length-1)*W, ty=v=>H-(v/80)*H;
    const pts=arr=>arr.map((v,i)=>`${tx(i)},${ty(v)}`).join(' ');
    const band=[...upper.map((v,i)=>`${tx(i)},${ty(v)}`), ...[...lower].reverse().map((v,i)=>`${tx(data.length-1-i)},${ty(v)}`)].join(' ');
    svg.innerHTML=`<polygon points="${band}" fill="rgba(0,99,56,.08)"/><polyline points="${pts(upper)}" fill="none" stroke="rgba(0,99,56,.2)" stroke-width="1" stroke-dasharray="3,3"/><polyline points="${pts(lower)}" fill="none" stroke="rgba(0,99,56,.2)" stroke-width="1" stroke-dasharray="3,3"/><polyline points="${pts(data)}" fill="none" stroke="#006338" stroke-width="2"/>`;
    const lEl=document.getElementById('projLabels'); if(lEl) lEl.innerHTML=['Wk1','Wk7','Wk13','Wk19','Wk26'].map(l=>`<span>${l}</span>`).join('');
  }

  /* ── HEATMAP ─────────────────────────────────────────────────── */
  function _updateHeatmap() {
    const el=document.getElementById('heatmap'); if(!el) return;
    if(!el.children.length) for(let i=0;i<50;i++){const d=document.createElement('div');d.className='hm-cell';el.appendChild(d);}
    const p=_state.sliders.pest, cy=_state.sliders.cyclone, fr=_state.sliders.frost;
    // Zone multipliers: cyclone peaks coastal (zone 0), frost peaks inland (zone 4)
    const cyM=[1.3,1.2,1.0,0.8,0.7], frM=[0.7,0.8,1.0,1.2,1.3];
    Array.from(el.children).forEach((c,i)=>{
      const zone=Math.floor(i/10);
      const zp=[p*.7,p*.8,p*.9,p*1.2,p*1.45][zone]||p;
      const val=Math.max(0,Math.min(100,zp+cy*0.10*cyM[zone]+fr*0.08*frM[zone]+(Math.random()-.5)*14));
      const t=val/100;
      c.style.background=`rgba(${Math.round(_lerp(0,239,t))},${Math.round(_lerp(99,68,t))},${Math.round(_lerp(56,68,t))},${(0.18+t*0.62).toFixed(2)})`;
    });
  }

  /* ── CHART.JS ANALYTICS ─────────────────────────────────────── */
  /* CF — Chart.js colour palette derived from CONFIG.theme */
  const CF = {
    green:  CONFIG.theme.green,    gL:   CONFIG.theme.greenLight,
    gold:   CONFIG.theme.gold,     gldL: CONFIG.theme.goldLight,
    mid:    '#2d5f4a',             ink:  CONFIG.theme.ink,
    orange: CONFIG.theme.orange,   red:  CONFIG.theme.red,
    border: CONFIG.theme.border,   text: CONFIG.theme.text,  dim: CONFIG.theme.dim,
    sans:   CONFIG.theme.fontSans, mono: CONFIG.theme.fontMono
  };

  /* Chart.defaults — applied ONCE globally */
  function _applyChartDefaults() {
    if (typeof Chart === 'undefined' || _applyChartDefaults._done) return;
    _applyChartDefaults._done = true;
    Chart.defaults.font.family  = CF.sans;
    Chart.defaults.font.size    = 14;
    Chart.defaults.font.weight  = '500';
    Chart.defaults.color        = '#2a3530';
    Chart.defaults.borderColor  = '#e5ebe7';
    Chart.defaults.elements.line.borderWidth  = 2.5;
    Chart.defaults.elements.line.tension      = 0.35;
    Chart.defaults.elements.point.radius      = 4;
    Chart.defaults.elements.point.hoverRadius = 7;
    Chart.defaults.plugins.legend.labels.boxWidth  = 12;
    Chart.defaults.plugins.legend.labels.boxHeight = 12;
    Chart.defaults.plugins.legend.labels.padding   = 18;
    Chart.defaults.plugins.legend.labels.font = { family:CF.sans, size:13, weight:'500' };
    Chart.defaults.plugins.tooltip.backgroundColor = '#0a1410';
    Chart.defaults.plugins.tooltip.titleColor      = '#fafaf7';
    Chart.defaults.plugins.tooltip.bodyColor       = 'rgba(250,250,247,.8)';
    Chart.defaults.plugins.tooltip.titleFont = { family:CF.sans, size:13, weight:'600' };
    Chart.defaults.plugins.tooltip.bodyFont  = { family:CF.sans, size:13 };
    Chart.defaults.plugins.tooltip.padding       = 12;
    Chart.defaults.plugins.tooltip.cornerRadius  = 8;
    Chart.defaults.plugins.tooltip.borderWidth   = 0;
  }

  const _CDef = () => ({
    responsive:true, maintainAspectRatio:false, animation:{duration:280},
    plugins:{
      legend:{display:false},
      tooltip:{
        backgroundColor:CF.ink, titleColor:'#f4f3ee', bodyColor:'rgba(244,243,238,.75)',
        titleFont:{family:CF.mono,size:11}, bodyFont:{family:CF.mono,size:11},
        padding:10, cornerRadius:2, borderWidth:0
      }
    },
    scales:{
      x:{ grid:{color:'rgba(14,20,16,.04)',tickLength:0,drawBorder:false}, border:{display:false},
          ticks:{color:CF.dim,font:{family:CF.sans,size:11}} },
      y:{ grid:{color:'rgba(14,20,16,.04)',tickLength:0,drawBorder:false}, border:{display:false},
          ticks:{color:CF.dim,font:{family:CF.sans,size:11}} }
    }
  });

  /* ── CHART REGISTRY — single source of truth for all Chart.js instances ── */
  const _ChartRegistry = {
    defs: {},
    instances: {},

    register(id, buildFn) { this.defs[id] = buildFn; },

    ensure(id) {
      let canvas = document.getElementById(id); if (!canvas) return;
      const parent = canvas.parentElement;
      const markEmpty = () => { if (parent) parent.classList.add('chart-empty'); canvas.classList.remove('chart-ready'); };
      const markReady = () => { if (parent) parent.classList.remove('chart-empty'); canvas.classList.add('chart-ready'); };
      if (typeof Chart === 'undefined') { markEmpty(); return; }
      // Abort if parent has no dimensions — still in display:none tree
      if (!parent || parent.offsetWidth === 0 || parent.offsetHeight === 0) { markEmpty(); return; }
      try {
        const existing = Chart.getChart(canvas);
        if (existing) existing.destroy();
        const fresh = document.createElement('canvas');
        fresh.id = id; canvas.replaceWith(fresh);
        const inst = new Chart(fresh, this.defs[id]());
        this.instances[id] = inst;
        if (parent) parent.classList.remove('chart-empty');
        fresh.classList.add('chart-ready');
        return inst;
      } catch(e) {
        console.warn('[chart] init failed for', id, e);
        markEmpty();
      }
    },

    ensureAll(ids) { ids.forEach(id => this.ensure(id)); },

    resizeAll() {
      if (typeof Chart === 'undefined') return;
      Object.values(this.instances).forEach(c => { try { c?.resize(); } catch(e){} });
      document.querySelectorAll('canvas').forEach(c => { try { Chart.getChart(c)?.resize(); } catch(e){} });
    }
  };

  /* Register build functions — runs once at IIFE init, before any DOM interaction */
  function _registerCharts() {
    const pR = Array(8).fill(0).map((_,i)=>i===0||i===7?4:0);
    const wks = ['Wk10','Wk11','Wk12','Wk13','Wk14','Wk15','Wk16','Wk17'];

    _ChartRegistry.register('biChart1', () => {
      const d=_CDef();
      const sgData=[96,94,95,93,91,94,96,94], gpData=[93,92,90,91,89,90,91,90];
      return {type:'line',data:{labels:wks,datasets:[
        {label:'SunGold G3',data:[...sgData],borderColor:CF.green,backgroundColor:'rgba(0,103,74,.12)',fill:true,tension:.35,borderWidth:2.5,pointRadius:pR,pointHoverRadius:7,pointHoverBorderWidth:2,pointBackgroundColor:CF.green},
        {label:'Green Pool', data:[...gpData],borderColor:CF.gold, backgroundColor:'rgba(201,169,97,.12)',fill:true,tension:.35,borderWidth:2.5,pointRadius:pR,pointHoverRadius:7,pointHoverBorderWidth:2,pointBackgroundColor:CF.gold}
      ]},options:{...d,scales:{...d.scales,y:{...d.scales.y,min:75,max:100,ticks:{...d.scales.y.ticks,callback:v=>v+'%'}}}}};
    });

    _ChartRegistry.register('biChart2', () => {
      const d=_CDef();
      return {type:'bar',data:{labels:['Katikati','Te Puke','Ōpōtiki'],datasets:[
        {label:'Cost of Delay NZD',data:[1200,800,2800],
         backgroundColor:[CF.gL,'rgba(14,20,16,.06)',CF.gldL],
         borderColor:[CF.green,CF.mid,CF.gold],borderWidth:1,borderRadius:6,borderSkipped:false,barPercentage:.55},
        {label:'Target',data:[1500,1500,1500],
         backgroundColor:'transparent',borderColor:'rgba(14,20,16,.25)',
         borderWidth:1,borderRadius:0,borderSkipped:false,barPercentage:.55,type:'bar'}
      ]},options:{...d,scales:{...d.scales,
        x:{...d.scales.x,ticks:{...d.scales.x.ticks,callback:v=>'$'+v.toLocaleString()}},
        y:{...d.scales.y,grid:{display:false}}
      },indexAxis:'y'}};
    });

    _ChartRegistry.register('biChart3', () => {
      const d=_CDef();
      const wk=Array.from({length:26},(_,i)=>'Wk'+(i+1));
      const base=[18,20,22,28,32,30,26,24,22,26,30,35,38,32,28,24,22,20,24,28,26,22,20,18,16,14];
      const b50u=base.map(v=>Math.min(100,v+6)),  b50l=base.map(v=>Math.max(0,v-6));
      const b80u=base.map(v=>Math.min(100,v+12)), b80l=base.map(v=>Math.max(0,v-12));
      const b95u=base.map(v=>Math.min(100,v+18)), b95l=base.map(v=>Math.max(0,v-18));
      return {type:'line',data:{labels:wk,datasets:[
        {data:[...b95u],borderColor:'transparent',backgroundColor:'rgba(0,103,74,.06)',fill:'+1',pointRadius:0,tension:.35},
        {data:[...b95l],borderColor:'transparent',fill:false,pointRadius:0,tension:.35},
        {data:[...b80u],borderColor:'transparent',backgroundColor:'rgba(0,103,74,.10)',fill:'+1',pointRadius:0,tension:.35},
        {data:[...b80l],borderColor:'transparent',fill:false,pointRadius:0,tension:.35},
        {data:[...b50u],borderColor:'transparent',backgroundColor:'rgba(0,103,74,.14)',fill:'+1',pointRadius:0,tension:.35},
        {data:[...b50l],borderColor:'transparent',fill:false,pointRadius:0,tension:.35},
        // dataset[6] — the live risk score line updated by _updateCharts()
        {label:'Risk Score',data:[...base],borderColor:CF.green,backgroundColor:'transparent',fill:false,tension:.35,pointRadius:0,pointHoverRadius:4,borderWidth:2.5}
      ]},options:{...d,scales:{...d.scales,y:{...d.scales.y,min:0,max:80,ticks:{...d.scales.y.ticks,callback:v=>v+'/100'}}}}};
    });

    _ChartRegistry.register('biChart4', () => {
      return {type:'doughnut',data:{
        labels:['Quality Bonus','Base Submit','Advance'],
        datasets:[{data:[53,320,80],
          backgroundColor:['#003d2b','#2d5f4a','rgba(0,61,43,.35)'],
          borderColor:'#ffffff',borderWidth:3}]
      },options:{responsive:true,maintainAspectRatio:false,animation:{duration:280},cutout:'68%',
        plugins:{
          legend:{display:true,position:'bottom',labels:{color:CF.text,font:{family:CF.sans,size:13},boxWidth:12,boxHeight:12,padding:18}},
          tooltip:_CDef().plugins.tooltip
        }
      },plugins:[{id:'centerLabel',beforeDraw(chart){
        const {ctx,width,height}=chart;
        const pay=_computePayment(_state.sliders.dm);
        ctx.save();
        ctx.font=`600 20px 'Fraunces','Times New Roman',serif`;
        ctx.fillStyle=CF.ink; ctx.textAlign='center'; ctx.textBaseline='middle';
        ctx.fillText('$'+pay.total.toFixed(2),width/2,height/2-8);
        ctx.font=`11px ${CF.sans}`; ctx.fillStyle=CF.dim;
        ctx.fillText('NZD/tray',width/2,height/2+12);
        ctx.restore();
      }}]};
    });

    _ChartRegistry.register('canvas90Day', () => {
      const s = _state.sliders, baseRisk = _computeRisk();
      const startWk = 17;
      const weeks = Array.from({ length: 13 }, (_, i) => 'Wk' + (startWk + i));
      const delta  = [0, 2, 5, 9, 14, 18, 14, 10, 6, 2, -2, -6, -10];
      const base   = delta.map(d => Math.min(99, Math.max(1, Math.round(baseRisk + d))));
      const bandOuter = base.map(v => Math.min(99, Math.round(v * 1.28)));
      const bandInner = base.map(v => Math.max(1,  Math.round(v * 0.78)));
      const otifMax = Math.max(40, Math.round(100 - s.cong * 0.35 - (s.rain > 40 ? (s.rain - 40) * 0.2 : 0)));
      const otifMin = Math.max(40, Math.round(otifMax * (1 - bandOuter[5] / 100)));
      const sumEl = document.getElementById('proj90Summary');
      if (sumEl) sumEl.textContent = `Expected OTIF range: ${otifMin}%–${otifMax}% through week ${startWk + 12} under current conditions.`;
      const milestones = [{ idx: 2, label: 'Peak MainPack' }, { idx: 6, label: 'Late Season' }, { idx: 10, label: 'Port Close' }];
      return {
        type: 'line',
        data: { labels: weeks, datasets: [
          { data: bandOuter, borderColor:'transparent', backgroundColor:'rgba(0,61,43,.10)', fill:'+1', tension:.35, pointRadius:0 },
          { data: bandInner, borderColor:'transparent', fill:false, tension:.35, pointRadius:0 },
          { data: base.map(v=>Math.min(99,Math.round(v*1.12))), borderColor:'transparent', backgroundColor:'rgba(0,61,43,.12)', fill:'+1', tension:.35, pointRadius:0 },
          { data: base.map(v=>Math.max(1, Math.round(v*0.90))), borderColor:'transparent', fill:false, tension:.35, pointRadius:0 },
          { label:'Projection', data:base, borderColor:'#003d2b', backgroundColor:'transparent', fill:false, tension:.35, pointRadius:0, pointHoverRadius:4, borderWidth:1.5 }
        ]},
        options: {
          responsive:true, maintainAspectRatio:false, animation:{ duration:280 },
          plugins:{
            legend:{ display:false },
            tooltip:{ backgroundColor:'#0e1410', titleColor:'#f4f3ee', bodyColor:'rgba(244,243,238,.75)',
              titleFont:{family:"'JetBrains Mono',monospace",size:11}, bodyFont:{family:"'JetBrains Mono',monospace",size:11},
              padding:10, cornerRadius:2, borderWidth:0 }
          },
          scales:{
            x:{ grid:{color:'rgba(14,20,16,.04)',tickLength:0,drawBorder:false}, border:{display:false},
                ticks:{color:'#8a938d',font:{family:"'Inter',sans-serif",size:11}} },
            y:{ suggestedMin:0, grid:{color:'rgba(14,20,16,.04)',tickLength:0,drawBorder:false}, border:{display:false},
                ticks:{color:'#8a938d',font:{family:"'Inter',sans-serif",size:11},callback:v=>v+'/100'} }
          }
        },
        plugins:[{
          id:'milestones',
          afterDraw(chart) {
            const {ctx, scales:{x,y}} = chart;
            milestones.forEach(({idx,label}) => {
              const px = x.getPixelForValue(idx);
              ctx.save();
              ctx.strokeStyle='rgba(161,98,7,.5)'; ctx.lineWidth=1; ctx.setLineDash([4,3]);
              ctx.beginPath(); ctx.moveTo(px,y.top); ctx.lineTo(px,y.bottom); ctx.stroke();
              ctx.fillStyle='#a16207'; ctx.font='10px '+CF.mono; ctx.textAlign='left';
              ctx.fillText(label, px+3, y.top+13);
              ctx.restore();
            });
          }
        }]
      };
    });
  }

  function _initCharts() {
    if (typeof Chart === 'undefined') return;
    _applyChartDefaults();
    _ChartRegistry.ensureAll(['biChart1','biChart2','biChart3','biChart4']);
    // Sync _state.charts aliases for _updateCharts()
    _state.charts.otif = _ChartRegistry.instances['biChart1'];
    _state.charts.cod  = _ChartRegistry.instances['biChart2'];
    _state.charts.risk = _ChartRegistry.instances['biChart3'];
    _state.charts.pool = _ChartRegistry.instances['biChart4'];
  }

  function _updateCharts() {
    if (typeof Chart === 'undefined') return;
    const s=_state.sliders, t=_state.tick;
    // Re-sync aliases after any registry re-ensure
    const otif = _ChartRegistry.instances['biChart1'] || _state.charts.otif;
    const cod  = _ChartRegistry.instances['biChart2'] || _state.charts.cod;
    const risk = _ChartRegistry.instances['biChart3'] || _state.charts.risk;
    const pool = _ChartRegistry.instances['biChart4'] || _state.charts.pool;

    if (otif && t%3===0) {
      const sg=Math.max(75,Math.round(98-(s.cong*0.3)-(s.rain>40?(s.rain-40)*0.15:0)));
      const gr=Math.max(70,sg-3);
      otif.data.datasets[0].data.push(sg); otif.data.datasets[0].data.shift();
      otif.data.datasets[1].data.push(gr); otif.data.datasets[1].data.shift();
      otif.update('none');
    }
    if (cod && t%4===0) {
      const ex=Math.max(0,s.dwell-12);
      cod.data.datasets[0].data=[1,0.78,2.5].map(f=>Math.round(f*312*ex+180));
      cod.update('none');
    }
    if (risk) {
      // dataset[6] is the Risk Score line (datasets 0-5 are the confidence bands — do NOT touch)
      risk.data.datasets[6].data=Array(26).fill(_computeRisk());
      risk.update('none');
    }
    if (pool && t%5===0) {
      const pay=_computePayment(s.dm);
      pool.data.datasets[0].data=[Math.round(pay.taste*100),Math.round(pay.submit*100),80];
      pool.update('none');
    }
  }

  /* ── REPORT GENERATOR ───────────────────────────────────────── */
  function _updateReport() {
    const s=_state.sliders, risk=_computeRisk(), pay=_computePayment(s.dm);
    const otif=Math.max(40,Math.round(100-(s.cong*0.35)-(s.rain>40?(s.rain-40)*0.2:0)));
    const ret=Math.round(384*(s.dm>=16.1?1.08:0.92)*(s.vol/100));
    const ex=Math.max(0,s.dwell-12), cod=Math.round(312*ex+7.5*ex);
    const rL=risk>60?'CRITICAL':risk>25?'ELEVATED':'LOW';
    const rec=risk>60?'Immediate corrective action required. Halt non-compliant submissions. Engage MPI liaison.':risk>25?'Elevated risk warrants increased monitoring. Review SH2 logistics and reduce dwell exposure.':'Conditions are optimal for export submission. Maintain current dry matter monitoring cadence. No corrective action required.';
    const el=document.getElementById('reportPreview'); if(!el) return;
    el.innerHTML=`<h2>APOPHENIA — Executive Briefing</h2><p>Operational Risk Intelligence · Season 2026 · Week 17</p><h3>Risk Assessment</h3><p>Predictive risk score: <strong>${risk}/100</strong> — ${rL} risk. ${pay.cat==='MTS Fail'?'<span class="crit">MTS FAILURE: market access blocked.</span>':'<span class="gold">'+pay.cat+'</span>'} — returning ${pay.rate}.</p><h3>Supply Chain Status</h3><p>OTIF: <strong>${otif}%</strong>. SH2 congestion ${s.cong}% — ${s.cong>60?'<span class="crit">critical delay risk.</span>':'within bounds.'}  Rainfall: ${s.rain}mm — ${s.rain>50?'heavy conditions.':'dry to moderate.'}</p><h3>Economic Impact</h3><p>Seasonal value: <strong>$${ret}M NZD</strong>. CoD exposure: $${Math.round(cod*2341/1e6)}M across 2,341 containers. ${s.dwell>24?'<span class="crit">Port dwell '+s.dwell+'h exceeds 24h threshold.</span>':'Port dwell within parameters.'}</p><h3>Senior Consultant Recommendation</h3><p>${rec}</p>`;
  }

  /* ── TAB SYSTEM ─────────────────────────────────────────────── */
  function _switchVaultTab(id) {
    document.querySelectorAll('.vault-tab-btn').forEach(b=>{
      const on=b.dataset.tab===id;
      b.classList.toggle('active',on); b.setAttribute('aria-selected',on?'true':'false'); b.setAttribute('tabindex',on?'0':'-1');
    });
    document.querySelectorAll('.vault-tab-panel').forEach(p=>{
      const on=p.id==='vault-tab-'+id;
      p.classList.toggle('active',on); p.setAttribute('aria-hidden',on?'false':'true');
    });
    if (id === 'analytics') {
      // Panel just became display:block — wait 2 frames for browser to assign dimensions
      requestAnimationFrame(() => requestAnimationFrame(() => {
        _ChartRegistry.ensureAll(['biChart1','biChart2','biChart3','biChart4']);
        _state.charts.otif = _ChartRegistry.instances['biChart1'];
        _state.charts.cod  = _ChartRegistry.instances['biChart2'];
        _state.charts.risk = _ChartRegistry.instances['biChart3'];
        _state.charts.pool = _ChartRegistry.instances['biChart4'];
        _drawProjectionSvg();
        _analyticsReady = true;
        requestAnimationFrame(() => _ChartRegistry.resizeAll());
      }));
    } else {
      _analyticsReady = false;
    }
  }

  /* ── SEASON DATA ─────────────────────────────────────────────── */
  const SEASONS = CONFIG.model.seasons;
  function _setSeason(s) {
    const d=SEASONS[s]||SEASONS['live'];
    Object.keys(_state.sliders).forEach(k=>{ if(d[k]!==undefined){_state.sliders[k]=d[k];} });
    Object.keys(_state.sliders).forEach(k=>{ const el=document.getElementById('s_'+k); if(el){el.value=_state.sliders[k]; _onSlider(k,el);} });
    const n=document.getElementById('seasonNote'); if(n){n.textContent=d.note; n.classList.toggle('show',!!d.note);}
    document.querySelectorAll('.season-btn').forEach(b=>b.classList.toggle('active',b.dataset.season===s));
    _drawProjectionSvg();
  }
  function _setPackhouse(m) {
    _state.packhouse=m;
    const mn=document.getElementById('ph_modern'), lg=document.getElementById('ph_legacy');
    if(mn) mn.className='ph-opt'+(m==='modern'?' active-modern':'');
    if(lg) lg.className='ph-opt'+(m==='legacy'?' active-legacy':'');
    _setText('ph_pte_display', CONFIG.copy.packhouse[m].readout);
    _updateDOM();
  }
  function _resetAll() {
    _setSeason('live'); _state.agents=[]; _state.totalReturn=0; _state.arrivedCount=0; _state.riskHistory=[]; _state.tick=0;
    _setPackhouse('modern'); _updateDOM(); _drawGis();
  }

  /* ── SIMULATION CONTROLS ────────────────────────────────────── */
  function _play() {
    if(_state.running) return; _state.running=true;
    _toggle('simPlayBtn','active',true); _toggle('simPauseBtn','active',false);
    _interval=setInterval(_tick, 1800/_state.speed);
  }
  function _pause() {
    _state.running=false; clearInterval(_interval);
    _toggle('simPlayBtn','active',false); _toggle('simPauseBtn','active',true);
  }
  function _setSpeed(sp) {
    _state.speed=sp;
    document.querySelectorAll('.sim-speed').forEach(b=>b.classList.toggle('active',+b.dataset.speed===sp));
    if(_state.running){clearInterval(_interval);_interval=setInterval(_tick,1800/sp);}
  }

  /* ── SLIDER HANDLER ─────────────────────────────────────────── */
  function _onSlider(key,el) {
    const val=_clamp(key,el.value); _state.sliders[key]=val;
    const _sdef = CONFIG.sliders.find(s => s.id === key); const unit = _sdef ? _sdef.unit : '';
    const d=document.getElementById('sv_'+key);
    if(d) d.innerHTML=val+`<span class="si-unit">${unit}</span>`;
    _updateDOM(); _drawGis(); _updateCharts(); _updateReport(); _updateHeatmap();
  }

  /* ── KEYBOARD TAB NAV ───────────────────────────────────────── */
  function _initTabKeys() {
    const tl=document.querySelector('[role="tablist"]'); if(!tl) return;
    tl.addEventListener('keydown',e=>{
      const tabs=Array.from(tl.querySelectorAll('[role="tab"]'));
      const cur=tabs.findIndex(t=>t===document.activeElement); if(cur<0) return;
      let next=-1;
      if(e.key==='ArrowRight') next=(cur+1)%tabs.length;
      if(e.key==='ArrowLeft')  next=(cur-1+tabs.length)%tabs.length;
      if(next>=0){e.preventDefault();tabs[next].focus();_switchVaultTab(tabs[next].dataset.tab);}
      if(e.key==='Enter'||e.key===' '){e.preventDefault();_switchVaultTab(tabs[cur].dataset.tab);}
    });
  }

  /* ── HELPERS ────────────────────────────────────────────────── */
  function _setText(id,v){const e=document.getElementById(id);if(e)e.textContent=v;}
  function _setVC(id,v,cls){const e=document.getElementById(id);if(!e)return;e.textContent=v;e.className=e.className.replace(/\b(green|warn|crit|gold)\b/g,'').trim()+' '+cls;}
  function _toggle(id,cls,on){const e=document.getElementById(id);if(e)e.classList.toggle(cls,on);}
  function _resizeAllCharts(){ _ChartRegistry.resizeAll(); }

  /* ── EVENT BINDING ──────────────────────────────────────────── */
  function _bind() {
    ['dm','pest','cong','rain','vol','reg','dwell','cyclone','frost'].forEach(k=>{
      const el=document.getElementById('s_'+k); if(el) el.addEventListener('input',()=>_onSlider(k,el));
    });
    document.querySelectorAll('.season-btn').forEach(b=>b.addEventListener('click',()=>_setSeason(b.dataset.season)));
    const phM=document.getElementById('ph_modern'), phL=document.getElementById('ph_legacy');
    if(phM) phM.addEventListener('click',()=>_setPackhouse('modern'));
    if(phL) phL.addEventListener('click',()=>_setPackhouse('legacy'));
    document.getElementById('resetBtn')?.addEventListener('click',_resetAll);
    document.getElementById('simPlayBtn')?.addEventListener('click',_play);
    document.getElementById('simPauseBtn')?.addEventListener('click',_pause);
    document.getElementById('simResetBtn')?.addEventListener('click',_resetAll);
    document.querySelectorAll('.sim-speed').forEach(b=>b.addEventListener('click',()=>_setSpeed(+b.dataset.speed)));
    document.querySelectorAll('.vault-tab-btn').forEach(b=>b.addEventListener('click',()=>_switchVaultTab(b.dataset.tab)));
    /* Hamburger */
    const ham=document.getElementById('hamburgerBtn'), ov=document.getElementById('navOverlay'), sb=document.getElementById('sidebar');
    if(ham) ham.addEventListener('click',()=>{
      const open=sb?.classList.toggle('open');
      document.body.classList.toggle('sidebar-open',open);
      ham.setAttribute('aria-expanded',open?'true':'false');
      ov?.classList.toggle('show',open);
    });
    if(ov) ov.addEventListener('click',()=>{
      sb?.classList.remove('open'); ov.classList.remove('show');
      document.body.classList.remove('sidebar-open');
      ham?.setAttribute('aria-expanded','false');
    });
    /* Report buttons — with visual feedback + scroll into view + error guard */
    const _flashBtn = (btn, label) => {
      if (!btn) return;
      const orig = btn.textContent;
      btn.textContent = label || '✓ Generated';
      btn.disabled = true;
      setTimeout(() => { btn.textContent = orig; btn.disabled = false; }, 1400);
    };
    const _runReport = (btn) => {
      try { _updateReport(); } catch(e) { console.warn('[report] update failed', e); }
      const preview = document.getElementById('reportPreview');
      if (preview) {
        preview.style.transition = 'opacity .25s';
        preview.style.opacity = '0.4';
        setTimeout(() => { preview.style.opacity = '1'; }, 180);
        if (typeof preview.scrollIntoView === 'function') {
          preview.scrollIntoView({ behavior:'smooth', block:'nearest' });
        }
      }
      _flashBtn(btn);
    };
    document.getElementById('generateReportBtn')?.addEventListener('click', (e) => {
      _switchVaultTab('briefing');
      _runReport(e.currentTarget);
    });
    document.getElementById('regenReportBtn')?.addEventListener('click', (e) => {
      _runReport(e.currentTarget);
    });
    document.getElementById('copyReportBtn')?.addEventListener('click',()=>{
      const el=document.getElementById('reportPreview'); const btn=document.getElementById('copyReportBtn');
      if(el&&navigator.clipboard){navigator.clipboard.writeText(el.innerText).catch(()=>{});if(btn){btn.textContent='✓ Copied';setTimeout(()=>btn.textContent='⎘ Copy Markdown',2000);}}
    });
    /* Nav active state + mobile close */
    document.querySelectorAll('.sb-nav-item').forEach(a=>a.addEventListener('click',()=>{
      document.querySelectorAll('.sb-nav-item').forEach(x=>x.classList.remove('active')); a.classList.add('active');
      if(window.innerWidth<900){sb?.classList.remove('open');ov?.classList.remove('show');document.body.classList.remove('sidebar-open');ham?.setAttribute('aria-expanded','false');}
    }));
    _initTabKeys();
    document.getElementById('dynChartGenerate')?.addEventListener('click', _generateDynChart);
    /* Toggle helpers — inline style for guaranteed visual feedback */
    const _setPillActive = (btn, on) => {
      if (!btn) return;
      btn.classList.toggle('active', on);
      btn.style.background = on ? 'var(--cf-ink, #0e1410)' : '';
      btn.style.color      = on ? 'var(--txt-on-dark, #fafaf7)' : '';
      btn.style.borderColor= on ? 'var(--cf-ink, #0e1410)' : '';
    };
    /* Auto-refresh toggle — actually polls live ETL feed */
    (function() {
      const btn = document.getElementById('autoRefreshBtn'); if (!btn) return;
      let _arTimer = null;
      _setPillActive(btn, btn.classList.contains('active'));
      btn.addEventListener('click', () => {
        const on = !btn.classList.contains('active');
        _setPillActive(btn, on);
        if (on) {
          _arTimer = setInterval(() => {
            try { if (typeof _Feed !== 'undefined' && _Feed && typeof _Feed.load === 'function') _Feed.load(); } catch(e){}
            try { _updateDOM(); } catch(e){}
            try { _updateCharts && _updateCharts(); } catch(e){}
            try { _drawArcGauge(_computeRisk()); } catch(e){}
          }, 30000);
        } else {
          clearInterval(_arTimer); _arTimer = null;
        }
      });
    })();
    window.addEventListener('resize', () => requestAnimationFrame(_resizeAllCharts));
    /* Chat */
    const _doChat = () => {
      const inp = document.getElementById('chatInput'); const msg = inp?.value.trim(); if (!msg) return;
      _chatPost('user', msg); inp.value = ''; inp.style.height = 'auto'; _chatRespond(msg);
    };
    document.getElementById('chatSend')?.addEventListener('click', _doChat);
    document.getElementById('chatInput')?.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); _doChat(); } });
    document.querySelectorAll('.qp-btn').forEach(b => b.addEventListener('click', () => { _chatPost('user', b.textContent); _chatRespond(b.textContent); }));
    /* PDF export */
    document.querySelectorAll('#exportPdfBtn, #exportPdfPill').forEach(b => b?.addEventListener('click', _exportPdf));
    /* Forecast Lab toggle — both buttons get robust visual highlight + show distinct content */
    const _btn26 = document.getElementById('btn26Week');
    const _btn90 = document.getElementById('btn90Day');
    _setPillActive(_btn26, _btn26?.classList.contains('active'));
    _setPillActive(_btn90, _btn90?.classList.contains('active'));
    _btn90?.addEventListener('click', () => {
      _setPillActive(_btn90, true);
      _setPillActive(_btn26, false);
      _show90DayModal();
    });
    _btn26?.addEventListener('click', () => {
      _setPillActive(_btn26, true);
      _setPillActive(_btn90, false);
      _close90DayModal();
      try { _drawProjectionSvg(); } catch(e){}
      // Scroll the projection into view so user sees the change
      const proj = document.getElementById('projSvg');
      if (proj && typeof proj.scrollIntoView === 'function') {
        proj.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  }

  /* ── ANALYTICS INTERSECTION OBSERVER ───────────────────────── */
  function _watchAnalyticsPanel() {
    const panel = document.getElementById('vault-tab-analytics');
    if (!panel) return;
    if (_analyticsObserver) _analyticsObserver.disconnect();
    _analyticsObserver = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting && !_analyticsReady) {
        _analyticsReady = true;
        _ChartRegistry.ensureAll(['biChart1','biChart2','biChart3','biChart4']);
        _state.charts.otif = _ChartRegistry.instances['biChart1'];
        _state.charts.cod  = _ChartRegistry.instances['biChart2'];
        _state.charts.risk = _ChartRegistry.instances['biChart3'];
        _state.charts.pool = _ChartRegistry.instances['biChart4'];
        _drawProjectionSvg();
        setTimeout(() => _ChartRegistry.resizeAll(), 80);
      } else if (!entries[0].isIntersecting) {
        _analyticsReady = false;
      }
    }, { threshold: 0.1 });
    _analyticsObserver.observe(panel);
  }

  /* ── MAP INTERSECTION OBSERVER ─────────────────────────────── */
  let _mapObserver = null;

  function _watchMapPanel() {
    const panel = document.getElementById('apoMap');
    if (!panel) return;
    if (_mapObserver) _mapObserver.disconnect();
    _mapObserver = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting && !_apoMapInited) {
        _mapObserver.disconnect();
        _initMapbox();
      }
    }, { threshold: 0.1 });
    _mapObserver.observe(panel);
  }

  /* ── INIT ───────────────────────────────────────────────────── */
  function _init() {
    _registerCharts(); // register all build functions before any DOM interaction
    _bind(); _initGis(); _updateDOM(); _updateHeatmap(); _drawArcGauge(24);
    _drawProjectionSvg(); _updateReport();
    _watchAnalyticsPanel();
    _watchMapPanel();
    setTimeout(_play, 900);
    setInterval(_updateHeatmap, 4200);
    // Non-blocking: fire-and-forget; never delays render
    _Feed.load();
  }


  /* ── DYNAMIC INTELLIGENCE CHART RENDERER ───────────────────── */
  let _dynChart = null;
  let _analyticsObserver = null;
  let _analyticsReady = false;
  const _MONO = "'JetBrains Mono','SF Mono',monospace";

  function _dynOpts() {
    return {
      responsive:true, maintainAspectRatio:false, animation:{duration:380},
      plugins:{
        legend:{display:true,position:'top',labels:{color:CF.text,font:{family:_MONO,size:13},padding:12}},
        tooltip:{backgroundColor:'rgba(255,255,255,.97)',titleColor:CF.ink,bodyColor:CF.text,
          borderColor:CF.border,borderWidth:1,titleFont:{family:_MONO,size:12},bodyFont:{family:_MONO,size:11}}
      },
      scales:{
        x:{grid:{color:CF.gL},ticks:{color:CF.dim,font:{family:_MONO,size:12}}},
        y:{grid:{color:CF.gL},ticks:{color:CF.dim,font:{family:_MONO,size:12}}}
      }
    };
  }

  const _DYN_CHART_DEFS = {
    otif_corridor:{
      title:'OTIF by Corridor', src:'SH2 Corridor / Synthetic',
      build(s){
        const o=Math.max(40,Math.round(100-(s.cong*0.35)-(s.rain>40?(s.rain-40)*0.2:0)));
        const d=_dynOpts();
        return {type:'bar',data:{
          labels:['Katikati–Tauranga','Te Puke–Tauranga','Ōpōtiki–Tauranga'],
          datasets:[{label:'OTIF %',data:[Math.round(o*1.03),Math.round(o*0.98),Math.round(o*0.91)],
            backgroundColor:[CF.gL,CF.gL,CF.gldL],
            borderColor:[CF.green,CF.green,CF.gold],borderWidth:1.5,borderRadius:4}]
        },options:{...d,scales:{...d.scales,y:{...d.scales.y,min:40,max:100,
          ticks:{...d.scales.y.ticks,callback:v=>v+'%'}}}}};
      }
    },
    dm_subzone:{
      title:'Dry Matter % by Subzone', src:'Source: QM · 2026',
      build(s){
        const b=s.dm, sz=_subzoneData;
        const vals=sz
          ? [
              sz['Katikati']  ? +(sz['Katikati'].dm_mean.toFixed(2))  : +(b+0.14).toFixed(2),
              sz['Pongakawa'] ? +(sz['Pongakawa'].dm_mean.toFixed(2)) : +(b+0.07).toFixed(2),
              sz['Te Puke']   ? +(sz['Te Puke'].dm_mean.toFixed(2))   : +b.toFixed(2),
              +(b-0.04).toFixed(2),
              sz['Opotiki']   ? +(sz['Opotiki'].dm_mean.toFixed(2))   : +(b-0.24).toFixed(2)
            ]
          : [+(b+0.14).toFixed(2),+(b+0.07).toFixed(2),+b.toFixed(2),+(b-0.04).toFixed(2),+(b-0.24).toFixed(2)];
        const cols=vals.map(v=>v>=16.1?CF.gL:v>=15.5?'rgba(180,83,9,.15)':'rgba(153,27,27,.15)');
        const bcols=vals.map(v=>v>=16.1?CF.green:v>=15.5?CF.orange:CF.red);
        const d=_dynOpts();
        return {type:'bar',data:{
          labels:['Katikati','Pongakawa','Te Puke','Tauranga','Ōpōtiki'],
          datasets:[{label:'DM Mean %',data:vals,indexAxis:'y',backgroundColor:cols,borderColor:bcols,borderWidth:1.5,borderRadius:4}]
        },options:{...d,indexAxis:'y',scales:{x:{...d.scales.x,min:14,max:20,
          ticks:{...d.scales.x.ticks,callback:v=>v+'%'}}}}};
      }
    },
    return_packweek:{
      title:'Financial Return by Pack Week', src:'Grower Payments 2026',
      build(s){
        const pay=s.dm>=17?4.40:s.dm>=16.1?4.13:s.dm>=15.5?3.20:0;
        const data=Array.from({length:12},(_,i)=>+(pay*(0.85+0.025*i)*(1-(s.cong/100)*0.15)).toFixed(2));
        const d=_dynOpts();
        return {type:'line',data:{
          labels:['Wk13','Wk14','Wk15','Wk16','Wk17','Wk18','Wk19','Wk20','Wk21','Wk22','Wk23','Wk24'],
          datasets:[{label:'NZD/tray',data,borderColor:CF.green,backgroundColor:CF.gL,fill:true,tension:.4,pointRadius:3}]
        },options:{...d,scales:{...d.scales,y:{...d.scales.y,ticks:{...d.scales.y.ticks,callback:v=>'$'+v.toFixed(2)}}}}};
      }
    },
    risk_trend:{
      title:'Risk Score Trend + Critical Threshold', src:'APO predictive model',
      build(s,risk){
        const hist=_state.riskHistory.length>1?[..._state.riskHistory]:Array(8).fill(risk);
        while(hist.length<8) hist.unshift(hist[0]);
        const wks=Array.from({length:hist.length},(_,i)=>'T-'+(hist.length-1-i));
        const d=_dynOpts();
        return {type:'line',data:{labels:wks,datasets:[
          {label:'Risk Score',data:hist,borderColor:CF.green,backgroundColor:CF.gL,fill:true,tension:.4,pointRadius:3},
          {label:'Critical (60)',data:Array(hist.length).fill(60),borderColor:CF.red,borderDash:[6,3],pointRadius:0,fill:false,borderWidth:1.5}
        ]},options:{...d,scales:{...d.scales,y:{...d.scales.y,min:0,max:100,ticks:{...d.scales.y.ticks,callback:v=>v+'/100'}}}}};
      }
    },
    payment_pool:{
      title:'Grower Payment Pool Split', src:'Grower Payments Booklet 2026',
      build(s){
        const taste=s.dm>=17?80:s.dm>=16.1?53:0;
        const base=s.dm>=15.5?320:0;
        const adv=s.dm>=15.5?80:0;
        return {type:'doughnut',data:{
          labels:['Quality Bonus','Base Submit','Advance'],
          datasets:[{data:[taste,base,adv],
            backgroundColor:[CF.gold,CF.green,CF.mid],
            borderColor:'#f8faf8',borderWidth:3}]
        },options:{responsive:true,maintainAspectRatio:false,animation:{duration:400},cutout:'60%',
          plugins:{legend:{display:true,position:'bottom',labels:{color:CF.text,font:{family:_MONO,size:13},padding:12}},
            tooltip:{backgroundColor:'rgba(255,255,255,.97)',titleColor:CF.ink,bodyColor:CF.text,borderColor:CF.border,borderWidth:1}}}};
      }
    },
    mts_subzone:{
      title:'MTS Pass/Fail Rate by Subzone', src:'Source: QM · 2026',
      build(s){
        const b=s.dm, sz=_subzoneData;
        const adj=v=>Math.min(100,Math.max(0,Math.round(v-Math.max(0,(16.1-b)*20))));
        const pass=sz
          ? [
              sz['Katikati']  ? Math.round(sz['Katikati'].mts_pass_rate  * 100) : adj(94),
              sz['Pongakawa'] ? Math.round(sz['Pongakawa'].mts_pass_rate * 100) : adj(91),
              sz['Te Puke']   ? Math.round(sz['Te Puke'].mts_pass_rate   * 100) : adj(89),
              adj(87),
              sz['Opotiki']   ? Math.round(sz['Opotiki'].mts_pass_rate   * 100) : adj(80)
            ]
          : [94,91,89,87,80].map(adj);
        const d=_dynOpts();
        return {type:'bar',data:{
          labels:['Katikati','Pongakawa','Te Puke','Tauranga','Ōpōtiki'],
          datasets:[
            {label:'MTS Pass %',data:pass,backgroundColor:'rgba(0,61,43,.65)',borderRadius:4},
            {label:'MTS Fail %',data:pass.map(v=>100-v),backgroundColor:'rgba(153,27,27,.45)',borderRadius:4}
          ]
        },options:{...d,scales:{
          x:{...d.scales.x,stacked:true},
          y:{...d.scales.y,stacked:true,min:0,max:100,ticks:{...d.scales.y.ticks,callback:v=>v+'%'}}}}};
      }
    }
  };

  function _generateDynChart() {
    if (typeof Chart === 'undefined') { console.warn('[APO] Chart.js not loaded'); return; }
    const sel = document.getElementById('dynChartSelector');
    const key = sel ? sel.value : 'otif_corridor';
    const def = _DYN_CHART_DEFS[key];
    if (!def) return;
    const st = APO.getState();
    const s  = { dm:st.dm, cong:st.cong, rain:st.rain, vol:st.vol, reg:st.reg, dwell:st.dwell||18 };
    const canvas = document.getElementById('dynChartCanvas');
    const empty  = document.getElementById('dynChartEmpty');
    const meta   = document.getElementById('dynChartMeta');
    if (!canvas) return;
    if (_dynChart) { _dynChart.destroy(); _dynChart = null; }
    canvas.style.display = 'block';
    if (empty) empty.style.display = 'none';
    if (meta)  meta.style.display  = 'flex';
    _setText('dynChartTitle', def.title);
    _setText('dynChartSrc',   def.src);
    // double-rAF: canvas.display just changed — wait for browser to assign dimensions
    requestAnimationFrame(() => requestAnimationFrame(() => {
      if (_dynChart) { _dynChart.destroy(); _dynChart = null; }
      _dynChart = new Chart(canvas, def.build(s, st.risk));
    }));
  }

  /* ── PUBLIC: setState — used by APO.Feed._inject ────────────── */
  function _setState(key, val) {
    if (!(key in _state.sliders)) return;
    _state.sliders[key] = _clamp(key, val);
    const el = document.getElementById('s_' + key);
    if (el) { el.value = _state.sliders[key]; el.setAttribute('aria-valuenow', _state.sliders[key]); }
    const _sdef2 = CONFIG.sliders.find(s => s.id === key); const unit = _sdef2 ? _sdef2.unit : '';
    const d = document.getElementById('sv_' + key);
    if (d) d.innerHTML = _state.sliders[key] + `<span class="si-unit">${unit}</span>`;
  }

  /* ── APO.Feed — live payload loader ─────────────────────────── */
  const _PAYLOAD_PATHS = CONFIG.api.payloadPaths;

  async function _loadPayload() {
    for (const url of _PAYLOAD_PATHS) {
      try {
        const res = await fetch(url + '?t=' + Date.now(), { cache: 'no-store' });
        if (res.ok) return await res.json();
      } catch {}
    }
    console.warn('[APO.Feed] Payload unavailable — using hardcoded defaults.');
    return null;
  }

  const _Feed = {
    async load() {
      const data = await _loadPayload();
      if (!data) return;
      try {
        this._validate(data);
        this._inject(data);
        this._updateTickerBadge(data);
      } catch (e) {
        console.warn('[APO.Feed] Payload inject failed:', e.message);
      }
    },

    _validate(data) {
      // Sanitise every external value before it touches the model
      const clamp = (v, min, max) => Math.min(max, Math.max(min, Number(v) || min));
      data.dm         = clamp(data.dm,         10,  25);
      data.pest       = clamp(data.pest,         0, 100);
      data.congestion = clamp(data.congestion,   0, 100);
      data.rainfall   = clamp(data.rainfall,     0, 300);
      data.vol        = clamp(data.vol,          50, 200);
      data.regulatory = clamp(data.regulatory,   0, 100);
    },

    _inject(data) {
      _setState('dm',   data.dm);
      _setState('cong', data.congestion);
      _setState('rain', data.rainfall);
      _setState('vol',  data.vol);
      _setState('reg',  data.regulatory);
      // Trigger full model refresh after all sliders are updated
      _updateDOM(); _drawGis(); _updateCharts(); _updateReport();
      // Subzone ETL data → per-corridor GIS agent DM + Dynamic Charts
      if (data.subzones)        _setSubzones(data.subzones);
      // Real weekly risk arc from ETL → projection SVG
      if (data.weekly_risk_arc) _drawProjectionSvg(data.weekly_risk_arc);
      if (data.fx) console.info('[APO.Feed] FX rates:', data.fx.nzd_eur, 'EUR /', data.fx.nzd_jpy, 'JPY');
      document.dispatchEvent(new CustomEvent('apo:synced'));
    },

    _updateTickerBadge(data) {
      const sysEl = document.getElementById('sysStatusText');
      if (sysEl) {
        const clim = data.rain_source === 'open-meteo-live' ? 'Climate live' : 'Data only';
        sysEl.textContent = `Data live · ${data.season || '2025/26'} PW${data.pack_week || 17} · ${clim}`;
      }
      // FX display element (shown in topbar if present)
      if (data.fx) {
        const fxEl = document.getElementById('fxDisplay');
        if (fxEl) {
          fxEl.style.display = 'flex';
          fxEl.textContent = `NZD/EUR ${data.fx.nzd_eur.toFixed(4)} · NZD/JPY ${data.fx.nzd_jpy.toFixed(2)}`;
        }
      }
    }
  };

  /* ── 90-DAY PROJECTION MODAL ────────────────────────────────── */
  let _projChart90 = null;

  function _show90DayModal() {
    let modal = document.getElementById('modal90Day');
    if (!modal) {
      const el = document.createElement('div');
      el.id = 'modal90Day';
      el.style.cssText = 'display:none;position:fixed;inset:0;z-index:9999;background:rgba(26,35,25,.72);backdrop-filter:blur(3px);align-items:center;justify-content:center;';
      el.innerHTML =
        '<div style="background:var(--cf-card);border:1px solid var(--cf-border);border-radius:var(--r2);width:min(760px,96vw);max-height:90vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.22);">' +
          '<div style="padding:18px 20px 0;display:flex;justify-content:space-between;align-items:flex-start;">' +
            '<div>' +
              '<div style="font-size:11px;letter-spacing:2px;color:var(--txt-dim);text-transform:uppercase;margin-bottom:4px;">90-Day Forecast</div>' +
              '<div style="font-size:var(--text-md);font-weight:700;color:var(--txt-primary);font-family:var(--f-serif);">Forward Risk Projection</div>' +
              '<div id="proj90Summary" style="font-size:13px;color:var(--txt-secondary);margin-top:4px;"></div>' +
            '</div>' +
            '<button id="close90Day" style="font-size:18px;color:var(--txt-dim);padding:4px 8px;border-radius:var(--r);line-height:1;cursor:pointer;" aria-label="Close">✕</button>' +
          '</div>' +
          '<div style="padding:14px 20px;height:320px;"><canvas id="canvas90Day"></canvas></div>' +
          '<div style="padding:4px 20px 18px;display:flex;gap:20px;flex-wrap:wrap;align-items:center;">' +
            '<span style="display:flex;align-items:center;gap:6px;font-size:11px;color:var(--txt-secondary);"><span style="width:20px;height:8px;background:rgba(0,61,43,.18);display:inline-block;border-radius:1px;"></span>90% confidence interval</span>' +
            '<span style="display:flex;align-items:center;gap:6px;font-size:11px;color:var(--txt-secondary);"><span style="width:20px;height:1.5px;background:#003d2b;display:inline-block;"></span>Base projection</span>' +
            '<span style="margin-left:auto;font-size:10px;color:var(--txt-dim);font-family:\'JetBrains Mono\',monospace;">Milestones — hairline</span>' +
          '</div>' +
        '</div>';
      document.body.appendChild(el);
      document.getElementById('close90Day')?.addEventListener('click', _close90DayModal);
      el.addEventListener('click', e => { if (e.target === el) _close90DayModal(); });
      modal = el;
    }

    modal.style.display = 'flex';

    if (_ChartRegistry.instances['canvas90Day']) {
      _ChartRegistry.instances['canvas90Day'].destroy();
      delete _ChartRegistry.instances['canvas90Day'];
    }

    const oldCanvas = document.getElementById('canvas90Day');
    if (oldCanvas) {
      const fresh = document.createElement('canvas');
      fresh.id = 'canvas90Day';
      fresh.style.cssText = 'display:block;width:100%;height:100%;';
      oldCanvas.replaceWith(fresh);
    }

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          const canvas = document.getElementById('canvas90Day');
          const parent = canvas.parentElement;

          if (parent.offsetWidth === 0 || parent.offsetHeight === 0) {
            console.warn('[90day] Parent still 0×0 after 3 rAFs', {
              w: parent.offsetWidth,
              h: parent.offsetHeight,
              display: getComputedStyle(parent).display
            });
            return;
          }

          const config = _ChartRegistry.defs['canvas90Day']();
          _ChartRegistry.instances['canvas90Day'] = new Chart(canvas, config);
        });
      });
    });
  }

  function _close90DayModal() {
    const modal = document.getElementById('modal90Day');
    if (modal) modal.style.display = 'none';
    if (_ChartRegistry.instances['canvas90Day']) {
      _ChartRegistry.instances['canvas90Day'].destroy();
      delete _ChartRegistry.instances['canvas90Day'];
    }
    if (_projChart90) { _projChart90.destroy(); _projChart90 = null; }
    const b26 = document.getElementById('btn26Week'), b90 = document.getElementById('btn90Day');
    if (b26) { b26.classList.add('active'); b26.style.background='var(--cf-ink, #0e1410)'; b26.style.color='var(--txt-on-dark, #fafaf7)'; b26.style.borderColor='var(--cf-ink, #0e1410)'; }
    if (b90) { b90.classList.remove('active'); b90.style.background=''; b90.style.color=''; b90.style.borderColor=''; }
  }

  function _build90DayChart() {
    if (typeof Chart === 'undefined') return;
    const canvas = document.getElementById('canvas90Day'); if (!canvas) return;
    _projChart90 = null;
    const s = _state.sliders, baseRisk = _computeRisk();
    const startWk = 17;
    const weeks = Array.from({ length: 13 }, (_, i) => 'Wk' + (startWk + i));
    const delta  = [0, 2, 5, 9, 14, 18, 14, 10, 6, 2, -2, -6, -10];
    const base   = delta.map(d => Math.min(99, Math.max(1, Math.round(baseRisk + d))));
    // Fan chart: optimistic/stress as confidence bands around base
    const bandOuter = base.map(v => Math.min(99, Math.round(v * 1.28)));
    const bandInner = base.map(v => Math.max(1,  Math.round(v * 0.78)));
    const otifMax = Math.max(40, Math.round(100 - s.cong * 0.35 - (s.rain > 40 ? (s.rain - 40) * 0.2 : 0)));
    const otifMin = Math.max(40, Math.round(otifMax * (1 - bandOuter[5] / 100)));
    const sumEl = document.getElementById('proj90Summary');
    if (sumEl) sumEl.textContent = `Expected OTIF range: ${otifMin}%–${otifMax}% through week ${startWk + 12} under current conditions.`;
    const milestones = [{ idx: 2, label: 'Peak MainPack' }, { idx: 6, label: 'Late Season' }, { idx: 10, label: 'Port Close' }];
    _projChart90 = new Chart(canvas, {
      type: 'line',
      data: { labels: weeks, datasets: [
        /* outer band upper edge — filled to next */
        { data: bandOuter, borderColor:'transparent', backgroundColor:'rgba(0,61,43,.10)', fill:'+1', tension:.35, pointRadius:0 },
        /* outer band lower edge */
        { data: bandInner, borderColor:'transparent', fill:false, tension:.35, pointRadius:0 },
        /* inner band upper edge — tighter fill */
        { data: base.map(v=>Math.min(99,Math.round(v*1.12))), borderColor:'transparent', backgroundColor:'rgba(0,61,43,.12)', fill:'+1', tension:.35, pointRadius:0 },
        { data: base.map(v=>Math.max(1, Math.round(v*0.90))), borderColor:'transparent', fill:false, tension:.35, pointRadius:0 },
        /* base line */
        { label:'Projection', data:base, borderColor:'#003d2b', backgroundColor:'transparent', fill:false, tension:.35, pointRadius:0, pointHoverRadius:4, borderWidth:1.5 }
      ]},
      options: {
        responsive:true, maintainAspectRatio:false, animation:{ duration:280 },
        plugins:{
          legend:{ display:false },
          tooltip:{ backgroundColor:'#0e1410', titleColor:'#f4f3ee', bodyColor:'rgba(244,243,238,.75)',
            titleFont:{family:"'JetBrains Mono',monospace",size:11}, bodyFont:{family:"'JetBrains Mono',monospace",size:11},
            padding:10, cornerRadius:2, borderWidth:0 }
        },
        scales:{
          x:{ grid:{color:'rgba(14,20,16,.04)',tickLength:0,drawBorder:false}, border:{display:false},
              ticks:{color:'#8a938d',font:{family:"'Inter',sans-serif",size:11}} },
          y:{ suggestedMin:0, grid:{color:'rgba(14,20,16,.04)',tickLength:0,drawBorder:false}, border:{display:false},
              ticks:{color:'#8a938d',font:{family:"'Inter',sans-serif",size:11},callback:v=>v+'/100'} }
        }
      },
      plugins:[{
        id:'milestones',
        afterDraw(chart) {
          const {ctx, scales:{x,y}} = chart;
          milestones.forEach(({idx,label}) => {
            const px = x.getPixelForValue(idx);
            ctx.save();
            ctx.strokeStyle='rgba(161,98,7,.5)'; ctx.lineWidth=1; ctx.setLineDash([4,3]);
            ctx.beginPath(); ctx.moveTo(px,y.top); ctx.lineTo(px,y.bottom); ctx.stroke();
            ctx.fillStyle='#a16207'; ctx.font='10px '+font; ctx.textAlign='left';
            ctx.fillText(label, px+3, y.top+13);
            ctx.restore();
          });
        }
      }]
    });
    console.log('[APO.90d] datasets assigned:', _projChart90?.data?.datasets?.map(ds=>({label:ds.label,data:ds.data})));
  }

  /* ── PDF EXPORT ─────────────────────────────────────────────── */
  function _exportPdf() {
    const JSPDF_CDN = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
    const _generate = () => {
      const { jsPDF } = window.jspdf;
      const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
      const s = _state.sliders, risk = _computeRisk(), pay = _computePayment(s.dm);
      const otif  = Math.max(40, Math.round(100 - s.cong * 0.35 - (s.rain > 40 ? (s.rain - 40) * 0.2 : 0)));
      const riskLv = risk < 30 ? 'LOW' : risk < 60 ? 'MEDIUM' : risk < 80 ? 'HIGH' : 'CRITICAL';
      const today  = new Date().toLocaleDateString('en-NZ', { day: '2-digit', month: 'long', year: 'numeric' });
      const mtsPass = Math.round(s.dm >= 15.5 ? Math.max(0, 94 - Math.max(0, (16.1 - s.dm) * 20)) : 0);
      const sz = _subzoneData;
      /* Header bar */
      doc.setFillColor(0, 61, 43); doc.rect(0, 0, 210, 18, 'F');
      doc.setTextColor(255, 255, 255); doc.setFont('helvetica', 'bold'); doc.setFontSize(14);
      doc.text('APOPHENIA', 14, 9);
      doc.setFontSize(9); doc.setFont('helvetica', 'normal');
      doc.text('Operational Risk Intelligence  ·  2026 SEASON', 14, 14);
      doc.text('DEMONSTRATION', 196, 12, { align: 'right' });
      /* Title */
      doc.setTextColor(26, 35, 25); doc.setFont('helvetica', 'bold'); doc.setFontSize(17);
      doc.text('Executive Intelligence Report', 14, 30);
      doc.setFontSize(10); doc.setFont('helvetica', 'normal'); doc.setTextColor(74, 94, 74);
      doc.text('Generated: ' + today + '  ·  Season 2025/26  ·  Pack Week 17', 14, 37);
      doc.setDrawColor(226, 232, 226); doc.line(14, 40, 196, 40);
      /* Risk */
      doc.setFont('helvetica', 'bold'); doc.setFontSize(12); doc.setTextColor(26, 35, 25);
      doc.text('Risk Assessment', 14, 50);
      const rCol = risk < 30 ? [0,99,56] : risk < 60 ? [249,115,22] : [239,68,68];
      doc.setFillColor(...rCol); doc.roundedRect(14, 53, 46, 13, 2, 2, 'F');
      doc.setTextColor(255, 255, 255); doc.setFontSize(17); doc.setFont('helvetica', 'bold');
      doc.text(risk + '/100', 37, 62, { align: 'center' });
      doc.setTextColor(26, 35, 25); doc.setFontSize(10); doc.setFont('helvetica', 'normal');
      doc.text('Risk Level: ' + riskLv, 66, 58);
      doc.text(risk < 30 ? 'Operations within normal parameters.' : risk < 60 ? 'Monitor conditions closely.' : 'Immediate review required.', 66, 64);
      doc.line(14, 72, 196, 72);
      /* Key Metrics */
      doc.setFont('helvetica', 'bold'); doc.setFontSize(12); doc.setTextColor(26, 35, 25);
      doc.text('Key Metrics', 14, 80);
      [['Dry Matter %', s.dm + '%', s.dm >= 16.1 ? 'SunGold G3' : s.dm >= 15.5 ? 'Green MTS' : 'Below MTS'],
       ['MTS Pass Rate', mtsPass + '%', mtsPass >= 90 ? 'Target met' : 'Below target'],
       ['OTIF Performance', otif + '%', otif >= 90 ? 'On target' : 'At risk'],
       ['Return per Tray', 'NZD $' + pay.total.toFixed(2), pay.cat]
      ].forEach(([lbl, val, note], i) => {
        const y = 88 + i * 9;
        doc.setFont('helvetica', 'normal'); doc.setTextColor(74, 94, 74); doc.text(lbl, 14, y);
        doc.setFont('helvetica', 'bold');   doc.setTextColor(26, 35, 25); doc.text(val, 90, y);
        doc.setFont('helvetica', 'normal'); doc.setTextColor(107,127,107); doc.text(note, 130, y);
      });
      doc.line(14, 127, 196, 127);
      /* Subzone table */
      doc.setFont('helvetica', 'bold'); doc.setFontSize(12); doc.setTextColor(26, 35, 25);
      doc.text('Subzone Breakdown', 14, 135);
      doc.setFillColor(240, 244, 240); doc.rect(14, 138, 182, 8, 'F');
      doc.setFontSize(9); doc.setFont('helvetica', 'bold'); doc.setTextColor(74, 94, 74);
      doc.text('Subzone', 16, 143); doc.text('Dry Matter Mean', 90, 143); doc.text('MTS Fail Rate', 148, 143);
      [['Katikati',  sz?.['Katikati']  ? sz['Katikati'].dm_mean.toFixed(2)  : (s.dm+0.14).toFixed(2), sz?.['Katikati']  ? Math.round((1-sz['Katikati'].mts_pass_rate)*100)+'%'  : '6%'],
       ['Pongakawa', sz?.['Pongakawa'] ? sz['Pongakawa'].dm_mean.toFixed(2) : (s.dm+0.07).toFixed(2), sz?.['Pongakawa'] ? Math.round((1-sz['Pongakawa'].mts_pass_rate)*100)+'%' : '9%'],
       ['Te Puke',   sz?.['Te Puke']   ? sz['Te Puke'].dm_mean.toFixed(2)   : s.dm.toFixed(2),         sz?.['Te Puke']   ? Math.round((1-sz['Te Puke'].mts_pass_rate)*100)+'%'   : '11%'],
       ['Tauranga',  (s.dm-0.04).toFixed(2), '13%'],
       ['Opotiki', sz?.['Opotiki'] ? sz['Opotiki'].dm_mean.toFixed(2) : (s.dm-0.24).toFixed(2), sz?.['Opotiki'] ? Math.round((1-sz['Opotiki'].mts_pass_rate)*100)+'%' : '20%']
      ].forEach(([name, dm, fail], i) => {
        const y = 151 + i * 8;
        doc.setFont('helvetica', 'normal'); doc.setTextColor(26, 35, 25);
        doc.text(name, 16, y); doc.text(dm + '%', 90, y); doc.text(fail, 148, y);
        if (i < 4) { doc.setDrawColor(226, 232, 226); doc.line(14, y + 2, 196, y + 2); }
      });
      doc.line(14, 193, 196, 193);
      /* Payment pool */
      doc.setFont('helvetica', 'bold'); doc.setFontSize(12); doc.setTextColor(26, 35, 25);
      doc.text('Payment Pool Summary', 14, 201);
      doc.setFontSize(10);
      [['Payment Tier', pay.cat],
       ['Base Submit Rate', 'NZD $' + pay.submit.toFixed(2) + '/tray'],
       ['Quality Bonus',     'NZD $' + pay.taste.toFixed(2)  + '/tray'],
       ['Total Return',     'NZD $' + pay.total.toFixed(2)  + '/tray'],
       ['Estimated Season Pool', 'NZD $' + Math.round(pay.total * s.vol) + 'M']
      ].forEach(([lbl, val], i) => {
        const y = 209 + i * 8;
        doc.setFont('helvetica', 'normal'); doc.setTextColor(74, 94, 74);  doc.text(lbl, 14, y);
        doc.setFont('helvetica', 'bold');   doc.setTextColor(26, 35, 25); doc.text(val, 100, y);
      });
      /* Footer */
      doc.setFillColor(240, 244, 240); doc.rect(0, 280, 210, 17, 'F');
      doc.setTextColor(107, 127, 107); doc.setFont('helvetica', 'normal'); doc.setFontSize(8);
      doc.text('Generated by APOPHENIA · Independent Operational Analytics', 105, 288, { align: 'center' });
      doc.text('Season 2025/26 · Demonstration version — independent research · synthetic data · not affiliated with any grower organisation', 105, 293, { align: 'center' });
      doc.save('APOPHENIA_Report_2025-26_PW17.pdf');
    };
    if (window.jspdf) { _generate(); return; }
    const sc = document.createElement('script');
    sc.src = JSPDF_CDN; sc.onload = _generate;
    sc.onerror = () => console.warn('[APO] jsPDF CDN failed to load');
    document.head.appendChild(sc);
  }

  /* ── LOCAL INTELLIGENCE ENGINE ─────────────────────────────── */
  // TO UPGRADE: replace APO.Chat.respond() with Claude API call
  // See SECURITY.md for API integration instructions
  const _Chat = {
    respond(msg) {
      const s = _state.sliders, risk = _computeRisk(), pay = _computePayment(s.dm);
      const otif = Math.max(40, Math.round(100 - s.cong * 0.35 - (s.rain > 40 ? (s.rain - 40) * 0.2 : 0)));
      const cod  = Math.round(risk * 3.12);
      const m = msg.toLowerCase();
      if (m.includes('risk')) {
        const lv = risk < 30 ? 'Low' : risk < 60 ? 'Medium' : 'High';
        return `Current risk is ${lv} — ${risk}/100. ${s.dm >= 16.1 ? 'Dry Matter above MTS threshold.' : 'WARNING: Dry Matter below MTS threshold.'} ${lv === 'Low' ? 'Operations within normal parameters.' : 'Review corridor conditions immediately.'}`;
      }
      if (m.includes('mts')) {
        return `MTS threshold is 16.1% Dry Matter. Current DM: ${s.dm}% — ${s.dm >= 16.1 ? 'PASS. Premium tier payments active.' : 'FAIL. Payments suspended until threshold met.'}`;
      }
      if (m.includes('tzg') || m.includes('payment')) {
        const pool = Math.round(pay.total * s.vol);
        return `Current payment tier: ${pay.cat}. Return per tray: NZD $${pay.total.toFixed(2)}. Estimated pool: NZD $${pool}M across all corridors.`;
      }
      if (m.includes('otif') || m.includes('corridor')) {
        return `OTIF performance: ${otif}% across active corridors. Ōpōtiki showing highest transit risk (VSI 2.5, 97km). Katikati performing best (VSI 1.6, 52km).`;
      }
      if (m.includes('cod') || m.includes('delay') || m.includes('cost')) {
        return `Cost of Delay estimated at NZD $${cod}K under current congestion levels. Reduce SH2 congestion index below 25% to minimise exposure.`;
      }
      return `Current status: Risk ${risk}/100 · DM ${s.dm}% · OTIF ${otif}%. Ask me about risk, MTS status, Premium tier payments, corridor OTIF, or Cost of Delay.`;
    }
  };

  function _chatPost(role, text) {
    const box = document.getElementById('chatMessages'); if (!box) return;
    const d = document.createElement('div'); d.className = 'msg ' + role;
    d.innerHTML = '<div class="msg-bubble">' + text.replace(/\n/g, '<br>') + '</div>';
    box.appendChild(d); box.scrollTop = box.scrollHeight;
  }
  function _chatRespond(msg) {
    const ti = document.getElementById('typingIndicator'), box = document.getElementById('chatMessages');
    if (ti) { ti.classList.add('show'); if (box) box.scrollTop = box.scrollHeight; }
    setTimeout(() => { if (ti) ti.classList.remove('show'); _chatPost('agent', _Chat.respond(msg)); }, 300);
  }

  /* ── PUBLIC API (read-only surface) ─────────────────────────── */
  return Object.freeze({
    init:           _init,
    switchVaultTab: _switchVaultTab,
    setSeasonMode:  _setSeason,
    setPackhouse:   _setPackhouse,
    resetAll:       _resetAll,
    setState:       _setState,
    Feed:           _Feed,
    Chat:           _Chat,
    initMap:        _initMapbox,
    refreshTraffic: _refreshNZTA,
    getState:       () => Object.freeze({ ...Object.assign({},_state.sliders), risk:_computeRisk(), tick:_state.tick, agents:_state.agents.length })
  });
})();

document.addEventListener('DOMContentLoaded', APO.init);

/* ── CONTROLS DRAWER ──────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  const sb    = document.getElementById('sidebar');
  const scrim = document.getElementById('drawerScrim');
  const tog   = document.getElementById('controlsToggleBtn');

  function _openDrawer()  { sb?.classList.add('open');    scrim?.classList.add('show');    tog?.setAttribute('aria-expanded','true');  }
  function _closeDrawer() { sb?.classList.remove('open'); scrim?.classList.remove('show'); tog?.setAttribute('aria-expanded','false'); }

  tog?.addEventListener('click',   () => sb?.classList.contains('open') ? _closeDrawer() : _openDrawer());
  scrim?.addEventListener('click', _closeDrawer);
  document.addEventListener('keydown', e => { if (e.key === 'Escape') _closeDrawer(); });

  /* ── CHAT FAB ────────────────────────────────────────────────────── */
  const fab       = document.getElementById('chatFab');
  const chatPanel = document.getElementById('sidebar') ? document.querySelector('.ai-panel') : null;

  fab?.addEventListener('click', () => {
    const panel = document.querySelector('.ai-panel');
    if (!panel) return;
    const isOpen = panel.classList.toggle('open');
    fab.classList.toggle('panel-open', isOpen);
    fab.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    panel.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
  });
  document.addEventListener('click', e => {
    const panel = document.querySelector('.ai-panel');
    if (!panel || !panel.classList.contains('open')) return;
    if (!panel.contains(e.target) && e.target !== fab && !fab?.contains(e.target)) {
      panel.classList.remove('open');
      fab?.classList.remove('panel-open');
      fab?.setAttribute('aria-expanded','false');
    }
  });

  /* ── SCROLL REVEALS ──────────────────────────────────────────────── */
  if (window.IntersectionObserver && !window.matchMedia('(prefers-reduced-motion:reduce)').matches) {
    const revealObs = new IntersectionObserver((entries) => {
      entries.forEach(e => { if (e.isIntersecting) { e.target.classList.add('in-view'); revealObs.unobserve(e.target); } });
    }, { threshold: 0.12 });
    document.querySelectorAll('[data-reveal], [data-reveal-stagger]').forEach(el => revealObs.observe(el));
  } else {
    document.querySelectorAll('[data-reveal], [data-reveal-stagger]').forEach(el => el.classList.add('in-view'));
  }

  /* ── KPI COUNTER ANIMATION ───────────────────────────────────────── */
  function _animateCounter(el, target, duration, formatter) {
    if (window.matchMedia('(prefers-reduced-motion:reduce)').matches) { el.textContent = formatter(target); return; }
    const start = performance.now();
    const initial = parseFloat(el.dataset.current || '0');
    function tick(now) {
      const t = Math.min(1, (now - start) / duration);
      const eased = 1 - Math.pow(1 - t, 3);
      el.textContent = formatter(initial + (target - initial) * eased);
      if (t < 1) requestAnimationFrame(tick);
      else el.dataset.current = target;
    }
    requestAnimationFrame(tick);
  }

  const counterObs = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (!e.isIntersecting || e.target.dataset.animated) return;
      e.target.dataset.animated = 'true';
      const kpiVal = e.target.querySelector('.kpi-val, .kpi-value, .rg-score, .triad-cost-val');
      if (!kpiVal) return;
      const raw = parseFloat(kpiVal.textContent.replace(/[^0-9.]/g,''));
      if (!isNaN(raw) && raw > 0) {
        const prefix = kpiVal.textContent.match(/^[^0-9]*/)?.[0] || '';
        const suffix = kpiVal.textContent.match(/[^0-9.]*$/)?.[0] || '';
        _animateCounter(kpiVal, raw, 1200, v => prefix + v.toFixed(raw < 10 ? 1 : 0) + suffix);
      }
    });
  }, { threshold: 0.5 });
  document.querySelectorAll('.kpi-cell, .kpi-card, .triad-card').forEach(el => counterObs.observe(el));
});

/* ── CHART DEBUG UTILITY (uncomment to activate) ──────────────────
function _chartAudit(label) {
  const all = [...document.querySelectorAll('canvas')];
  console.log(`[AUDIT ${label}]`, all.map(c => ({
    id: c.id,
    parent: c.parentElement?.id || c.parentElement?.className,
    cssDisplay: getComputedStyle(c.parentElement).display,
    offsetW: c.offsetWidth, offsetH: c.offsetHeight,
    bitmapW: c.width, bitmapH: c.height,
    chart: !!Chart.getChart(c),
    chartDims: Chart.getChart(c) ? {w:Chart.getChart(c).width,h:Chart.getChart(c).height} : null
  })));
}
// Paste in DevTools console to run manually:
// _chartAudit('manual')
// [...document.querySelectorAll('canvas')].map(c=>({id:c.id,w:c.offsetWidth,h:c.offsetHeight,chart:!!Chart.getChart(c)}))
──────────────────────────────────────────────────────────────────── */
/* ── DOC MODAL + LAST SYNC ──────────────────────────────────── */
/* ── DOC MODAL + LAST SYNC — Sprint 9.2 ──────────────────────────── */
'use strict';

const _docContent = {
  /* ── Methodology ──────────────────────────────────────────────── */
  'methodology-risk': {
    eyebrow: 'METHODOLOGY',
    title: 'Risk model overview',
    body: `<p>APO v4 uses a multi-variable logistic regression to produce a composite risk score (0–100) from five operational inputs.</p>
      <h4>Input variables</h4>
      <ul>
        <li><strong>Dry matter</strong> — deviation from submission threshold (most sensitive driver)</li>
        <li><strong>Congestion</strong> — SH2 corridor load as % of capacity</li>
        <li><strong>Rainfall</strong> — 7-day NIWA forecast in mm above 15mm baseline</li>
        <li><strong>Volume</strong> — seasonal tray volume as % of baseline</li>
        <li><strong>Regulatory</strong> — MPI compliance load and inspection queue</li>
      </ul>
      <h4>Model output</h4>
      <p>Risk score drives the executive recommendation, OTIF projection, and corridor prioritisation. Updated on every slider interaction.</p>`
  },
  'methodology-otif': {
    eyebrow: 'METHODOLOGY',
    title: 'OTIF calculation basis',
    body: `<p>On-Time In-Full (OTIF) measures the percentage of export containers that reach Tauranga Port within the scheduled window and at full volume.</p>
      <h4>Drivers modelled</h4>
      <ul>
        <li>Corridor congestion (SH2): linear degradation above 40% load</li>
        <li>Rainfall: non-linear penalty above 40mm / 7-day</li>
        <li>Dwell time: compounding penalty when &gt;20h dwell × congestion &gt;55%</li>
      </ul>
      <h4>Baseline</h4>
      <p>Display OTIF average at default slider settings: 91% — a calibrated display formula, not tied to any specific season (see METHODOLOGY.md, "Display model vs validated model"). Risk model backtested against four synthetic seasons: McFadden pseudo-R² 0.7006 (MTS fail model) and 0.9394 (OTIF&lt;88% model).</p>`
  },
  'methodology-dm': {
    eyebrow: 'METHODOLOGY',
    title: 'Dry-matter weighting',
    body: `<p>Dry matter (DM%) is the strongest single risk predictor in the model. Below the MPI submission threshold, risk increases exponentially.</p>
      <h4>Weighting function</h4>
      <ul>
        <li>DM &lt; 15.5%: exponential penalty (up to +40 risk points)</li>
        <li>DM 15.5–16.1%: marginal risk (+10 points)</li>
        <li>DM ≥ 16.1%: no penalty</li>
      </ul>
      <p>Source: MPI Export Standard 2026 · public Grower Payments Booklet · NZKGI 2024 validation study.</p>`
  },
  'methodology-ci': {
    eyebrow: 'METHODOLOGY',
    title: 'Confidence intervals',
    body: `<p>The 26-week and 90-day projections carry explicit confidence bands to reflect model uncertainty.</p>
      <h4>Band construction</h4>
      <ul>
        <li>Outer 90% band: ±28% of base score</li>
        <li>Inner 80% band: ±12% of base score</li>
        <li>Bands widen beyond week 12 reflecting reduced forecast accuracy</li>
      </ul>
      <h4>Limitations</h4>
      <p>Bands assume stable regulatory environment and no black-swan events. Cyclone/frost scenarios should be modelled explicitly via the scenario sliders.</p>`
  },
  /* ── Data sources ──────────────────────────────────────────────── */
  'src-nzta': {
    eyebrow: 'TRAFFIC DATA',
    title: 'Live road traffic',
    body: `<dl class="meta-grid">
      <dt>Provider</dt><dd>Mapbox Traffic API · TomTom-sourced</dd>
      <dt>Coverage</dt><dd>Global, including New Zealand state highway network</dd>
      <dt>Refresh</dt><dd>~5 minute cadence (Mapbox-managed)</dd>
      <dt>Colour coding</dt><dd>Green (low) · Gold (moderate) · Orange (heavy) · Red (severe)</dd>
      <dt>Visualisation</dt><dd>Live congestion lines under corridor overlays</dd>
      <dt>Status</dt><dd><span class="status-live">&#x25CF; LIVE</span></dd>
    </dl>`
  },
  'src-niwa': {
    eyebrow: 'PUBLIC DATA SOURCE',
    title: 'BOP rainfall data',
    body: `<dl class="meta-grid">
      <dt>Provider</dt><dd>Open-Meteo (free weather API)</dd>
      <dt>Coordinates</dt><dd>Te Puke region, Bay of Plenty</dd>
      <dt>Forecast horizon</dt><dd>7 days</dd>
      <dt>Refresh</dt><dd>Hourly</dd>
      <dt>Type</dt><dd>Public, free API</dd>
      <dt>Status</dt><dd><span class="status-live">&#x25CF; PUBLIC API</span></dd>
    </dl>`
  },
  'src-industry': {
    eyebrow: 'SYNTHETIC DATASET',
    title: 'Operational inventory data (synthetic)',
    body: `<dl class="meta-grid">
      <dt>Source type</dt><dd>Calibrated synthetic dataset</dd>
      <dt>Calibration basis</dt><dd>Public industry quality manuals 2026<br>Grower Payments Booklet 2026 (public PDF)<br>Stats NZ Horticulture Survey aggregates</dd>
      <dt>Variables generated</dt><dd>DM readings (μ=16.37, σ=0.75)<br>Tray volumes (scaled to 120M baseline)<br>MTS pass rates, packhouse PTE</dd>
      <dt>Method</dt><dd>Stochastic generation within documented industry ranges</dd>
      <dt>Important</dt><dd>No proprietary operational data was accessed</dd>
      <dt>Status</dt><dd><span class="status-sync">&#x25D0; SYNTHETIC</span></dd>
    </dl>`
  },
  'src-apo': {
    eyebrow: 'RESEARCH MODEL',
    title: 'APO v4 risk model',
    body: `<dl class="meta-grid">
      <dt>Built by</dt><dd>Gabriela Olivera · Independent operational analytics</dd>
      <dt>Model type</dt><dd>Multi-variable risk regression with logistic transform</dd>
      <dt>Calibration</dt><dd>4-season backtest against synthetic dataset</dd>
      <dt>McFadden pseudo-R²</dt><dd>0.7006 (MTS fail model) · 0.9394 (OTIF&lt;88% model) — synthetic backtest only</dd>
      <dt>Use</dt><dd>Portfolio demonstration of operational risk modelling for fruit export supply chains.</dd>
      <dt>Status</dt><dd><span class="status-sync">&#x25D0; RESEARCH PROTOTYPE</span></dd>
    </dl>`
  },
  'src-public': {
    eyebrow: 'CONNECTED PUBLIC APIS',
    title: 'Real-time public data feeds',
    body: `<dl class="meta-grid">
      <dt>Open-Meteo</dt><dd>BOP rainfall and weather forecasts</dd>
      <dt>Frankfurter</dt><dd>NZD/EUR and NZD/JPY exchange rates</dd>
      <dt>Overpass</dt><dd>SH2 corridor geometry verification</dd>
      <dt>Stats NZ</dt><dd>Horticulture Survey aggregates (volume baselines)</dd>
      <dt>Refresh</dt><dd>Each public source on its own cadence</dd>
      <dt>Status</dt><dd><span class="status-live">&#x25CF; LIVE PUBLIC APIS</span></dd>
    </dl>`
  },
  'src-mapbox': {
    eyebrow: 'GEOSPATIAL DATA SOURCE',
    title: 'Mapbox · Maps & Live Traffic',
    body: `<dl class="meta-grid">
      <dt>Provider</dt><dd>Mapbox · TomTom (traffic data)</dd>
      <dt>Coverage</dt><dd>Global vector tiles, including New Zealand state highway network</dd>
      <dt>Use in APOPHENIA</dt><dd>Bay of Plenty corridor map, live traffic congestion overlay, packhouse markers</dd>
      <dt>Refresh</dt><dd>Vector tiles cached; traffic refreshed ~5 min by Mapbox</dd>
      <dt>Type</dt><dd>Public commercial API · free-tier deployment</dd>
      <dt>Status</dt><dd><span class="status-live">&#x25CF; LIVE</span></dd>
    </dl>`
  },
  /* ── Legal ─────────────────────────────────────────────────────── */
  'terms': {
    eyebrow: 'TERMS OF USE',
    title: 'Terms of use',
    body: `<p>APOPHENIA is an independent portfolio project demonstrating operational risk modelling, data engineering, and executive dashboard design applied to NZ kiwifruit export supply chains. Published under the MIT licence.</p>
      <h4>Permitted use</h4>
      <ul>
        <li>Professional review, portfolio demonstration, and research reference</li>
        <li>Scenario modelling and operational risk methodology illustration</li>
        <li>Educational and analytical exploration of supply chain risk models</li>
      </ul>
      <h4>Important notes</h4>
      <ul>
        <li>This is a demonstration prototype built on synthetic data — not validated for live operational decisions</li>
        <li>Not affiliated with, endorsed by, or commissioned by any grower organisation</li>
        <li>Source code available at github.com/gabrielaoliveranz under the MIT licence</li>
      </ul>`
  },
  'privacy': {
    eyebrow: 'DATA HANDLING',
    title: 'Data & privacy',
    body: `<p>APOPHENIA processes only publicly available data and locally generated synthetic datasets. No proprietary or personal data is collected.</p>
      <h4>Public data used</h4>
      <p>Open-Meteo (weather forecasts), Overpass / OpenStreetMap (corridor geometry), Frankfurter (FX rates), Stats NZ (horticulture aggregates), Mapbox (vector basemap and live traffic overlay).</p>
      <h4>Synthetic data</h4>
      <p>Operational inventory variables are generated stochastically within ranges documented in public industry publications. No real proprietary operational data was accessed.</p>
      <h4>No personal data</h4>
      <p>This prototype does not collect, store, or transmit any personal data.</p>`
  },
  'limitations': {
    eyebrow: 'MODEL DISCLAIMER',
    title: 'Model limitations & scope',
    body: `<div style="background:var(--cf-raised);border-left:3px solid var(--risk-mid);padding:var(--s2) var(--s3);margin-bottom:var(--s3);border-radius:var(--r);">
      <strong>Demonstration version</strong><br>
      This is a research prototype built on synthetic data. It is <strong>not</strong> validated for binding operational decisions without customisation to real proprietary data. Not affiliated with or endorsed by any grower organisation.
      </div>
      <h4>Reported accuracy (synthetic backtest)</h4>
      <ul><li>26-week risk arc: 90% confidence interval</li></ul>
      <h4>Scope for production deployment</h4>
      <ul><li>Replace synthetic feeds with live proprietary data sources</li><li>Re-calibrate model against actual historical OTIF records</li><li>Subzone granularity below packhouse level requires additional data integration</li></ul>`
  },
  'author': {
    eyebrow: 'ABOUT THE AUTHOR',
    title: 'Gabriela Olivera',
    body: `<div class="author-modal-card">
      <div class="author-modal-photo">
        <img src="assets/gabriela.webp" alt="Gabriela Olivera" loading="lazy">
      </div>
      <div class="author-modal-content">
        <p class="author-modal-role">Data Analyst · Operational Analytics · Tauranga, NZ</p>
        <p>20+ years of professional experience, with the last 14+ across operations, procurement, and data analytics — between Argentina and New Zealand. Specialised in turning operational complexity into data-driven decisions.</p>
        <p>APOPHENIA is an independent portfolio project demonstrating end-to-end data engineering, predictive modelling, and executive dashboard design — applied to NZ kiwifruit export supply chains.</p>
        <p class="author-modal-availability"><em>Currently exploring data analyst, BI, and analytics engineering opportunities across Aotearoa.</em></p>
        <div class="author-modal-links">
          <a href="https://www.linkedin.com/in/gabriela-olivera-nz" target="_blank" rel="noopener">LinkedIn</a>
          <a href="https://github.com/gabrielaoliveranz" target="_blank" rel="noopener">GitHub</a>
          <a href="https://kaggle.com/gabrielaoliveranz" target="_blank" rel="noopener">Kaggle</a>
          <button type="button" class="author-modal-email-pill" id="copyEmailModalBtn" data-email="gabriela.olivera.nz@gmail.com">
            <span class="pill-default">Email</span>
            <span class="pill-success">✓ Copied</span>
          </button>
        </div>
      </div>
    </div>`
  },
  'contact': {
    eyebrow: 'ABOUT THIS PROTOTYPE',
    title: 'APOPHENIA — Horticultural Export Risk Intelligence',
    body: `<dl class="meta-grid">
      <dt>Purpose</dt><dd>Independent portfolio project demonstrating end-to-end data engineering, predictive risk modelling, and executive dashboard design applied to NZ kiwifruit export supply chains.</dd>
      <dt>Data</dt><dd>Synthetic operational data generated from publicly available Bay of Plenty seasonal benchmarks. No proprietary grower data is used.</dd>
      <dt>Risk model</dt><dd>Logistic sigmoid function combining dry matter, SH2 congestion, rainfall, regulatory load, dwell time, and volume stress indicators.</dd>
      <dt>Stack</dt><dd>Vanilla HTML/CSS/JavaScript · Chart.js · Mapbox GL JS · jsPDF · Open-Meteo API</dd>
      <dt>Version</dt><dd>4.1.0 · Season 2025/26</dd>
      <dt>Disclaimer</dt><dd>Independent research. Not affiliated with any grower organisation, exporter, or government agency.</dd>
    </dl>`
  },
  'commercial': {
    eyebrow: 'CONTACT',
    title: 'Get in touch',
    body: `<p>Data Analyst exploring full-time and contract opportunities in operational analytics across Aotearoa.</p>
      <h4>Contact</h4>
      <p>Email: <a href="mailto:gabriela.olivera.nz@gmail.com"><strong>gabriela.olivera.nz@gmail.com</strong></a></p>
      <p>LinkedIn: <a href="https://www.linkedin.com/in/gabriela-olivera-nz" target="_blank" rel="noopener">linkedin.com/in/gabriela-olivera-nz</a></p>
      <p>GitHub: <a href="https://github.com/gabrielaoliveranz" target="_blank" rel="noopener">github.com/gabrielaoliveranz</a></p>
      <h4>Location</h4>
      <p>Tauranga, Bay of Plenty · Aotearoa New Zealand</p>
      <p style="margin-top:var(--s3); font-size:12px; color:var(--txt-dim); font-style:italic;">APOPHENIA is an independent portfolio project. Not affiliated with any grower organisation.</p>`
  },
};

function _openDoc(key) {
  const c = _docContent[key]; if (!c) return;
  document.getElementById('docModalEyebrow').textContent = c.eyebrow;
  document.getElementById('docModalTitle').textContent   = c.title;
  document.getElementById('docModalBody').innerHTML      = c.body;
  const modal = document.getElementById('docModal');
  modal.classList.add('open');
  modal.setAttribute('aria-hidden', 'false');
  modal.querySelector('.doc-modal-close')?.focus();
}
function _closeDoc() {
  const modal = document.getElementById('docModal');
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
}

document.addEventListener('DOMContentLoaded', () => {
  /* Wire all [data-doc] links */
  document.querySelectorAll('[data-doc]').forEach(el => {
    el.addEventListener('click', e => { e.preventDefault(); _openDoc(el.dataset.doc); });
  });
  /* Close triggers */
  document.querySelectorAll('[data-close]').forEach(el => {
    el.addEventListener('click', _closeDoc);
  });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') _closeDoc(); });

  /* ── Last sync timer ───────────────────────────────────────────── */
  let _lastSyncTime = new Date();

  function _updateLastSync() {
    const el = document.getElementById('lastSyncDisplay');
    if (!el) return;
    const now  = new Date();
    const minsAgo = Math.floor((now - _lastSyncTime) / 60000);
    const time = _lastSyncTime.toLocaleTimeString('en-NZ', {
      hour: '2-digit', minute: '2-digit', timeZone: 'Pacific/Auckland'
    });
    el.innerHTML = `v4.0 · Last sync <strong>${time} NZT</strong>`;
    el.classList.toggle('stale', minsAgo > 30);
  }

  document.addEventListener('apo:synced', () => {
    _lastSyncTime = new Date();
    _updateLastSync();
  });

  setInterval(_updateLastSync, 60000);
  _updateLastSync();
});

document.getElementById('copyEmailBtn')?.addEventListener('click', async (e) => {
  const btn = e.currentTarget;
  const email = btn.dataset.email;
  try {
    await navigator.clipboard.writeText(email);
  } catch {
    const tmp = document.createElement('textarea');
    tmp.value = email;
    document.body.appendChild(tmp);
    tmp.select();
    document.execCommand('copy');
    tmp.remove();
  }
  btn.classList.add('copied');
  setTimeout(() => btn.classList.remove('copied'), 1800);
});

/* T4 — Footer CTA buttons */
document.getElementById('ctaCommercial')?.addEventListener('click', () => _openDoc('commercial'));
document.getElementById('ctaAbout')?.addEventListener('click', () => _openDoc('author'));

/* T5 — Delegated copy handler for author modal email pill */
document.addEventListener('click', async (e) => {
  if (e.target.closest('#copyEmailModalBtn')) {
    const btn = e.target.closest('#copyEmailModalBtn');
    const email = btn.dataset.email;
    try {
      await navigator.clipboard.writeText(email);
    } catch {
      const tmp = document.createElement('textarea');
      tmp.value = email;
      document.body.appendChild(tmp);
      tmp.select();
      document.execCommand('copy');
      tmp.remove();
    }
    btn.classList.add('copied');
    setTimeout(() => btn.classList.remove('copied'), 1800);
  }
});