/* global React */
const { useState } = React;

// ── Brand header ─────────────────────────────────────────────────────────
function BrandHeader({ onBack, simTime }) {
  return (
    <div className="brand-header">
      <div>
        {onBack && (
          <div className="crumb" onClick={onBack} style={{marginBottom:8}}>
            ← Roster
          </div>
        )}
        <h1 className="brand-title">🏭&nbsp;&nbsp;VMI Automation</h1>
        <div className="brand-sub">
          Vendor-Managed Inventory — tank simulation, auto-planning, schedule parsing, alert emails
        </div>
      </div>
      <div style={{display:'flex', gap:8, alignItems:'flex-start'}}>
        {simTime && (
          <div className="simtime">
            <span className="lbl">Sim time</span>
            <span className="val">{simTime}</span>
          </div>
        )}
        <button className="btn btn-secondary btn-sm">💻 Codebase</button>
      </div>
    </div>
  );
}

// ── Section header ───────────────────────────────────────────────────────
function SectionH3({ children, right }) {
  return (
    <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:12}}>
      <h3 className="section-h3" style={{margin:0}}>{children}</h3>
      {right}
    </div>
  );
}

// ── Chip ─────────────────────────────────────────────────────────────────
function Chip({ kind = 'standby', children }) {
  return <span className={`chip chip-${kind}`}>{children}</span>;
}

// ── Alert banner ─────────────────────────────────────────────────────────
function Banner({ kind = 'info', children }) {
  return <div className={`banner banner-${kind}`}>{children}</div>;
}

// ── KPI card ─────────────────────────────────────────────────────────────
function Kpi({ label, value, delta }) {
  return (
    <div className="kpi">
      <div className="kpi-label">{label}</div>
      <div className="kpi-val">{value}</div>
      {delta && <div className="kpi-delta">{delta}</div>}
    </div>
  );
}

// ── Tank card with animated SVG ──────────────────────────────────────────
function TankCard({ name, level, capacity, status }) {
  const pct = Math.max(0, Math.min(1, level / capacity));
  const fillColor = pct < 0.2 ? '#F43F5E' : pct < 0.5 ? '#F59E0B' : '#0EA5E9';
  const dotClass  = pct < 0.2 ? 'dot-red' : pct < 0.5 ? 'dot-amber' : 'dot-green';
  const SVG_W = 60, SVG_H = 80;
  const tankLeft = 6, tankTop = 8, tankW = 48, tankH = 66;
  const fluidH = tankH * pct;
  const fluidY = tankTop + (tankH - fluidH);
  const id = `t_${name.replace(/[^A-Za-z0-9_]/g, '_')}`;
  const isDraw = status === 'draw';

  return (
    <div className="tank-card">
      <div className="tank-row">
        <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} width="56" height="74" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <clipPath id={`clip_${id}`}>
              <rect x={tankLeft} y={tankTop} rx="3" ry="3" width={tankW} height={tankH} />
            </clipPath>
          </defs>
          <rect x={tankLeft} y={tankTop} rx="3" ry="3" width={tankW} height={tankH}
                fill="#F8FAFC" stroke="#CBD5E1" strokeWidth="1.5" />
          <rect x={tankLeft} y={fluidY} width={tankW} height={fluidH}
                fill={fillColor} opacity="0.85" clipPath={`url(#clip_${id})`}
                style={{transition:'y 600ms cubic-bezier(0.4,0,0.2,1), height 600ms cubic-bezier(0.4,0,0.2,1)'}} />
          <ellipse cx={tankLeft + tankW/2} cy={tankTop} rx={tankW/2} ry="2.5"
                   fill="#E2E8F0" stroke="#CBD5E1" strokeWidth="1" />
        </svg>
        <div style={{flex:1, minWidth:0}}>
          <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', gap:6}}>
            <div style={{display:'flex', alignItems:'center', gap:6, minWidth:0}}>
              <span className={`dot ${dotClass}`} />
              <span className="tank-name">{name}</span>
            </div>
            <Chip kind={isDraw ? 'draw' : 'standby'}>{isDraw ? 'DRAW' : 'STANDBY'}</Chip>
          </div>
          <div style={{marginTop:6}}>
            <span className="tank-num">{level.toLocaleString()}</span>{' '}
            <span className="tank-meta">/ {capacity.toLocaleString()} lbs · {(pct*100).toFixed(0)}%</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Customer roster card ─────────────────────────────────────────────────
function CustomerCard({ name, subtitle, severity, count, onOpen, disabled }) {
  let chip;
  if (severity === 'danger') chip = <Chip kind="danger">🔴 {count} CRITICAL</Chip>;
  else if (severity === 'warning') chip = <Chip kind="warning">🟡 {count} WARNING</Chip>;
  else chip = <Chip kind="success">🟢 ALL CLEAR</Chip>;
  return (
    <div className="card" style={{display:'grid', gridTemplateColumns:'1fr auto', gap:14, alignItems:'center'}}>
      <div>
        <div style={{fontSize:'1.05rem', fontWeight:600, color: disabled ? '#475569' : '#0F172A'}}>{name}</div>
        <div className="secondary" style={{fontSize:'0.82rem', marginTop:2}}>{subtitle}</div>
        <div style={{marginTop:8}}>{chip}</div>
      </div>
      <button
        className={`btn ${disabled ? 'btn-secondary' : 'btn-primary'}`}
        disabled={disabled}
        onClick={onOpen}
        title={disabled ? 'Demo placeholder — not connected to data.' : ''}>
        {disabled ? 'View' : '▶ Open demo'}
      </button>
    </div>
  );
}

Object.assign(window, { BrandHeader, SectionH3, Chip, Banner, Kpi, TankCard, CustomerCard });
