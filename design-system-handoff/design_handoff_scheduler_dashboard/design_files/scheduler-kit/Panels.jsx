/* global React, Chip, TankCard, Kpi, SectionH3, Banner */
const { useState: useStateD } = React;

// ── Projection chart (SVG mini recreation of the Plotly chart) ──────────
function ProjectionChart({ product, traces, safety = 10000 }) {
  // traces: [{name, color, points: number[]}]
  const W = 520, H = 220, pad = { l: 36, r: 12, t: 18, b: 28 };
  const N = traces[0]?.points.length || 0;
  const maxY = 37000;
  const x = (i) => pad.l + (i / (N - 1)) * (W - pad.l - pad.r);
  const y = (v) => pad.t + (1 - v / maxY) * (H - pad.t - pad.b);
  const cutoffIdx = Math.floor(N * 0.45);
  const cutoffX = x(cutoffIdx);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{width:'100%', height:'auto', background:'#fff', borderRadius:8}} xmlns="http://www.w3.org/2000/svg">
      {/* Run window vrects (subtle blue) */}
      {[[0.05, 0.13], [0.18, 0.26], [0.31, 0.39]].map(([s, e], i) => (
        <rect key={i} x={x(s*N)} y={pad.t} width={x(e*N) - x(s*N)} height={H - pad.t - pad.b}
              fill="rgba(30,64,175,0.06)" />
      ))}
      {/* Y grid */}
      {[0, 10000, 20000, 30000].map(v => (
        <g key={v}>
          <line x1={pad.l} x2={W-pad.r} y1={y(v)} y2={y(v)} stroke="#E2E8F0" strokeWidth="1" />
          <text x={pad.l - 6} y={y(v) + 4} textAnchor="end" fontFamily="Inter" fontSize="9" fill="#64748B">{v ? `${v/1000}k` : '0'}</text>
        </g>
      ))}
      {/* Safety stock dotted */}
      <line x1={pad.l} x2={W-pad.r} y1={y(safety)} y2={y(safety)} stroke="#F43F5E" strokeWidth="1.2" strokeDasharray="2 3" />
      <text x={W - pad.r - 4} y={y(safety) - 4} textAnchor="end" fontFamily="Inter" fontSize="9" fill="#9F1239">Safety stock</text>
      {/* Forecast vline */}
      <line x1={cutoffX} x2={cutoffX} y1={pad.t} y2={H-pad.b} stroke="#94A3B8" strokeWidth="1" strokeDasharray="3 3" />
      <text x={cutoffX + 4} y={pad.t + 10} fontFamily="Inter" fontSize="9" fill="#64748B">forecast →</text>
      {/* Traces — solid then dotted */}
      {traces.map((tr, ti) => {
        const solidPts = tr.points.slice(0, cutoffIdx + 1).map((v, i) => `${x(i)},${y(v)}`).join(' ');
        const dottedPts = tr.points.slice(cutoffIdx).map((v, i) => `${x(cutoffIdx + i)},${y(v)}`).join(' ');
        return (
          <g key={ti}>
            <polyline points={solidPts} fill="none" stroke={tr.color} strokeWidth="2" />
            <polyline points={dottedPts} fill="none" stroke={tr.color} strokeWidth="2" strokeDasharray="3 3" />
          </g>
        );
      })}
      {/* X tick labels */}
      {['Mon', 'Wed', 'Fri', 'Mon+1', 'Fri+1'].map((d, i) => (
        <text key={i} x={x((i / 4) * (N - 1))} y={H - 8} fontFamily="Inter" fontSize="9" fill="#64748B" textAnchor="middle">{d}</text>
      ))}
      {/* Title + legend */}
      <text x={pad.l} y={12} fontFamily="Inter" fontSize="11" fontWeight="600" fill="#1E2A45">{product}</text>
      <g>
        {traces.map((tr, ti) => (
          <g key={ti} transform={`translate(${W - pad.r - 130 + ti*70}, 4)`}>
            <line x1="0" x2="14" y1="6" y2="6" stroke={tr.color} strokeWidth="2.5" />
            <text x="18" y="9" fontFamily="Inter" fontSize="9" fill="#1E2A45">{tr.name}</text>
          </g>
        ))}
      </g>
    </svg>
  );
}

// ── Schedule Parser panel ────────────────────────────────────────────────
function SchedulePanel() {
  const [text, setText] = useStateD(`Hi — next week's run schedule:
Mon 6:00–22:00
Tue 6:00–22:00
Wed 6:00–14:00
Fri 6:00–14:00

Thanks,
Acme Plant Ops`);
  const [windows, setWindows] = useStateD([
    { day: 'Mon', start: '06:00', end: '22:00' },
    { day: 'Tue', start: '06:00', end: '22:00' },
    { day: 'Wed', start: '06:00', end: '14:00' },
    { day: 'Fri', start: '06:00', end: '14:00' },
  ]);
  return (
    <div className="card">
      <SectionH3 right={<Chip kind="success">PARSED · HIGH CONFIDENCE</Chip>}>📅 Schedule Parser</SectionH3>
      <div className="row" style={{gap:14}}>
        <div className="col field">
          <label>Paste schedule email</label>
          <textarea rows="7" value={text} onChange={e => setText(e.target.value)} style={{fontFamily:'JetBrains Mono, monospace', fontSize:'0.8rem'}} />
          <div style={{display:'flex', gap:8, marginTop:8}}>
            <button className="btn btn-secondary btn-sm">🧪 Simulate HIGH</button>
            <button className="btn btn-secondary btn-sm">🧪 Simulate LOW</button>
          </div>
        </div>
        <div className="col">
          <div className="eyebrow" style={{marginBottom:6}}>Run windows · 4 detected</div>
          <table className="tbl">
            <thead><tr><th>Day</th><th>Start</th><th>End</th><th></th></tr></thead>
            <tbody>
              {windows.map((w, i) => (
                <tr key={i}>
                  <td><b>{w.day}</b></td>
                  <td className="num">{w.start}</td>
                  <td className="num">{w.end}</td>
                  <td style={{textAlign:'right'}}><button className="btn btn-ghost btn-sm">edit</button></td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{display:'flex', gap:8, marginTop:10, justifyContent:'flex-end'}}>
            <button className="btn btn-secondary btn-sm">Cancel</button>
            <button className="btn btn-primary btn-sm">Apply schedule</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Auto-Planner panel ───────────────────────────────────────────────────
function PlannerPanel() {
  const trucks = [
    { sap: 'SAP20001', product: 'Product U', qty: 33000, arrival: 'Tue 04-28 13:00', tank: 'U-Tank1' },
    { sap: 'SAP20002', product: 'Product M', qty: 37000, arrival: 'Wed 04-29 10:00', tank: 'M-Tank1+M-Tank2' },
    { sap: 'SAP20003', product: 'Product U', qty: 35000, arrival: 'Fri 05-01 08:00', tank: 'U-Tank2' },
  ];
  return (
    <div className="card">
      <SectionH3 right={<Chip kind="receiving">3 PROPOSED</Chip>}>🤖 Auto-Planner</SectionH3>
      <table className="tbl">
        <thead><tr><th>SAP #</th><th>Product</th><th>Qty</th><th>Arrival</th><th>Tank</th></tr></thead>
        <tbody>
          {trucks.map((t, i) => (
            <tr key={i}>
              <td className="num"><b>{t.sap}</b></td>
              <td>{t.product}</td>
              <td className="num">{t.qty.toLocaleString()} lbs</td>
              <td className="num">{t.arrival}</td>
              <td className="muted" style={{fontSize:'0.78rem'}}>{t.tank}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{display:'flex', gap:8, marginTop:10, justifyContent:'flex-end'}}>
        <button className="btn btn-secondary btn-sm">Edit plan</button>
        <button className="btn btn-primary btn-sm">Send to CS</button>
      </div>
    </div>
  );
}

// ── VMI Controls ─────────────────────────────────────────────────────────
function ControlsPanel() {
  const [auto, setAuto] = useStateD(true);
  const [tgtU, setTgtU] = useStateD(22000);
  const [tgtM, setTgtM] = useStateD(22000);
  return (
    <div className="card">
      <SectionH3>🎛️ VMI Controls</SectionH3>
      <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', padding:'4px 0 12px'}}>
        <div>
          <div style={{fontWeight:600, fontSize:'0.9rem'}}>Automation</div>
          <div className="muted" style={{fontSize:'0.78rem'}}>Auto-parse schedules & commit truck orders</div>
        </div>
        <label className="toggle">
          <input type="checkbox" checked={auto} onChange={() => setAuto(!auto)} />
          <span className="toggle-slider"></span>
        </label>
      </div>
      <div className="divider" />
      <div className="field" style={{marginBottom:14}}>
        <label>Reorder target — Product U <span className="mono secondary">{tgtU.toLocaleString()} lbs</span></label>
        <input type="range" min="12000" max="30000" step="500" value={tgtU} onChange={e => setTgtU(+e.target.value)} />
      </div>
      <div className="field">
        <label>Reorder target — Product M <span className="mono secondary">{tgtM.toLocaleString()} lbs</span></label>
        <input type="range" min="12000" max="30000" step="500" value={tgtM} onChange={e => setTgtM(+e.target.value)} />
      </div>
      <div style={{display:'flex', gap:8, marginTop:14, justifyContent:'flex-end'}}>
        <button className="btn btn-secondary btn-sm">Reset</button>
        <button className="btn btn-primary btn-sm">Apply</button>
      </div>
    </div>
  );
}

Object.assign(window, { ProjectionChart, SchedulePanel, PlannerPanel, ControlsPanel });
