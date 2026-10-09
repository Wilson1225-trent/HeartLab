// HeartLab server - zero dependencies. Run: node server.js  (Node 18+)
const http = require('http'), fs = require('fs'), path = require('path');
const PORT = process.env.PORT || 3000;

let beats = [];   // {t: ms epoch, rr: ms, hr: bpm|null, ectopic: bool}
let clients = new Set();
let vitals = [], events = [], counters = {}, lastX = {};

const median = a => { const s = [...a].sort((x, y) => x - y); return s[s.length >> 1]; };
const mean = a => a.reduce((x, y) => x + y, 0) / (a.length || 1);
const sd = a => { const m = mean(a); return Math.sqrt(mean(a.map(x => (x - m) ** 2))); };

function stats(n = 120) {
  const rr = beats.slice(-n).map(b => b.rr);
  if (rr.length < 5) return { n: rr.length };
  const diffs = rr.slice(1).map((v, i) => v - rr[i]);
  const rmssd = Math.sqrt(mean(diffs.map(d => d * d)));
  const sdnn = sd(rr);
  const sd1 = rmssd / Math.SQRT2;
  const sd2 = Math.sqrt(Math.max(0, 2 * sdnn ** 2 - 0.5 * rmssd ** 2));
  const ect = beats.slice(-n).filter(b => b.ectopic).length;
  return {
    n: rr.length, meanHR: +(60000 / mean(rr)).toFixed(1),
    minHR: +(60000 / Math.max(...rr)).toFixed(0), maxHR: +(60000 / Math.min(...rr)).toFixed(0),
    meanRR: +mean(rr).toFixed(0), sdnn: +sdnn.toFixed(1), rmssd: +rmssd.toFixed(1),
    pnn50: +(100 * diffs.filter(d => Math.abs(d) > 50).length / diffs.length).toFixed(1),
    sd1: +sd1.toFixed(1), sd2: +sd2.toFixed(1), sd1sd2: +(sd1 / (sd2 || 1)).toFixed(2),
    irregularBeats: ect, irregularPct: +(100 * ect / rr.length).toFixed(1),
    flag: ect / rr.length > 0.1 ? 'HIGH irregularity - consider talking to a clinician' :
          ect / rr.length > 0.03 ? 'some irregular beats' : 'regular'
  };
}

function ingest(p) {
  let t = (p.t ? p.t * 1000 : Date.now());
  const list = (p.rr || []).filter(v => v > 250 && v < 2500);
  // place beats backwards from "now" so they're evenly spaced in time
  let total = list.reduce((a, b) => a + b, 0), cur = t - total;
  const added = [];
  for (const rr of list) {
    cur += rr;
    const b = { t: cur, rr, hr: Math.round(60000 / rr), ectopic: false };
    // irregular = deviates >20% from median of the previous 8 beats
    const prev = beats.slice(-8).map(x => x.rr);
    if (prev.length >= 5 && Math.abs(rr - median(prev)) > 0.2 * median(prev)) b.ectopic = true;
    beats.push(b); added.push(b);
  }
  if (beats.length > 200000) beats = beats.slice(-100000);
  if (p.x) { lastX = { ...p.x, t }; vitals.push(lastX); if (vitals.length > 50000) vitals = vitals.slice(-25000); }
  const newEv = (p.ev || []).map(e => ({ k: e.k, v: e.v, t: (e.t || 0) * 1000 }));
  events.push(...newEv);
  if (p.c) counters = p.c;
  const msg = JSON.stringify({ hr: p.hr, added, stats: stats(), x: lastX, ev: newEv, c: counters });
  for (const c of clients) c.write(`data: ${msg}\n\n`);
}

function csv() {
  const head = 'time_iso,rr_ms,instant_hr_bpm,irregular_flag\n';
  return head + beats.map(b => `${new Date(b.t).toISOString()},${b.rr},${b.hr},${b.ectopic ? 1 : 0}`).join('\n');
}

const send = (res, code, type, body, extra = {}) => {
  res.writeHead(code, { 'Content-Type': type, 'Access-Control-Allow-Origin': '*', ...extra }); res.end(body);
};

http.createServer((req, res) => {
  if (req.method === 'OPTIONS') return send(res, 204, 'text/plain', '');
  if (req.url === '/ingest' && req.method === 'POST') {
    let d = ''; req.on('data', c => d += c);
    req.on('end', () => { try { ingest(JSON.parse(d)); send(res, 200, 'application/json', '{"ok":1}'); }
      catch (e) { send(res, 400, 'application/json', '{"ok":0}'); } });
    return;
  }
  if (req.url === '/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    res.write(`data: ${JSON.stringify({ history: beats.slice(-600), stats: stats(), x: lastX, ev: events.slice(-30), c: counters })}\n\n`);
    clients.add(res); req.on('close', () => clients.delete(res)); return;
  }
  if (req.url === '/export.csv')
    return send(res, 200, 'text/csv', csv(), { 'Content-Disposition': 'attachment; filename=heartlab.csv' });
  if (req.url === '/vitals.csv')
    return send(res, 200, 'text/csv', 'time_iso,spo2,temp_c,steps,calories,altitude_m,pressure_pa,cadence,speed_ms,motion,rmssd,watch_status\n' +
      vitals.map(v => [new Date(v.t).toISOString(), v.spo2, v.temp, v.steps, v.cal, v.alt, v.press, v.cad, v.spd, v.motion, v.rmssd, v.status].join(',')).join('\n'),
      { 'Content-Disposition': 'attachment; filename=heartlab_vitals.csv' });
  if (req.url === '/events.csv')
    return send(res, 200, 'text/csv', 'time_iso,event,value\n' + events.map(e => `${new Date(e.t).toISOString()},${e.k},${e.v}`).join('\n'),
      { 'Content-Disposition': 'attachment; filename=heartlab_events.csv' });
  if (req.url === '/report.json')
    return send(res, 200, 'application/json', JSON.stringify({
      disclaimer: 'Consumer-grade RR data (not a medical ECG). Irregularity flag is a simple heuristic.',
      summary: stats(100000), watch_counters: counters, events: events.slice(-200), latest_vitals: lastX, sample_beats: beats.slice(-300) }, null, 2));
  if (req.url === '/reset' && req.method === 'POST') { beats = []; return send(res, 200, 'text/plain', 'ok'); }
  const f = path.join(__dirname, 'public', 'index.html');
  send(res, 200, 'text/html', fs.readFileSync(f));
}).listen(PORT, () => console.log('HeartLab on :' + PORT));