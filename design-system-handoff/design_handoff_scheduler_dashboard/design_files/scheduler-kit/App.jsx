/* global React, ReactDOM, BrandHeader, SectionH3, Chip, Banner, Kpi, TankCard,
   CustomerCard, ProjectionChart, SchedulePanel, PlannerPanel, ControlsPanel */
const { useState } = React;

function Roster({ onOpen }) {
  return (
    <div className="app-shell">
      <BrandHeader />
      <div style={{margin:'0 0 16px'}}>
        <div className="eyebrow">Customer roster</div>
        <div className="secondary" style={{fontSize:'0.95rem', marginTop:4}}>
          Multi-customer view. Click <b>Customer 1 — Acme</b> to run the live demo.
        </div>
      </div>
      <div className="stack-md">
        <CustomerCard
          name="Customer 1 — Acme Plastics"
          subtitle="Live demo · Mon 06:00 → Sat 04:00 shift · 4 tanks · 2 products"
          severity="danger" count={2} onOpen={onOpen} />
        <CustomerCard
          name="Customer 2"
          subtitle="Example tenant — visualization only"
          severity="success" disabled />
        <CustomerCard
          name="Customer 3"
          subtitle="Example tenant — visualization only"
          severity="warning" count={1} disabled />
        <CustomerCard
          name="Customer 4"
          subtitle="Example tenant — visualization only"
          severity="danger" count={2} disabled />
      </div>
      <div className="muted" style={{fontSize:'0.8rem', marginTop:20, fontStyle:'italic'}}>
        Real customer onboarding is a JSON-config drop into <code>customers/</code>.
      </div>
    </div>
  );
}

function Dashboard({ onBack }) {
  const productU = [
    { name: 'U-Tank1', color: '#1E3A8A', points: [29000, 28200, 27000, 25500, 24800, 23800, 23000, 22500, 22000, 21500, 21500, 21500] },
    { name: 'U-Tank2', color: '#60A5FA', points: [1000, 1000, 1000, 1000, 1000, 30000, 29500, 29200, 28800, 28200, 27800, 27000] },
  ];
  const productM = [
    { name: 'M-Tank1', color: '#0F766E', points: [30446, 29900, 29200, 28500, 27800, 27000, 26200, 25500, 24800, 24000, 23200, 22500] },
    { name: 'M-Tank2', color: '#5EEAD4', points: [1000, 1000, 1000, 28000, 27500, 27000, 26500, 26000, 25500, 25000, 24500, 24000] },
  ];
  return (
    <div className="app-shell">
      <BrandHeader onBack={onBack} simTime="Tue 2026-04-28 13:00" />

      {/* ── Alerts ── */}
      <SectionH3 right={<Chip kind="danger">2 ACTIVE</Chip>}>🚨 Alerts</SectionH3>
      <div className="stack-sm" style={{marginBottom:24}}>
        <Banner kind="danger">
          <div><b>RED FLAG:</b> SAP20001 (Product U, 33,000 lbs) at Tue 2026-04-28 13:00 — projected space in U-Tank1 is 25,836 lbs. Delivery must fit in one tank. Arriving too early — reschedule later.</div>
        </Banner>
        <Banner kind="danger">
          <div><b>RED FLAG:</b> SAP20002 (Product M, 37,000 lbs) at Wed 2026-04-29 10:00 — projected combined tank space is 31,836 lbs (M-Tank1 + M-Tank2). Truck cannot fit across both tanks. Reschedule or delay.</div>
        </Banner>
      </div>

      {/* ── 12-Day Projection ── */}
      <SectionH3>📈 12-Day Projection</SectionH3>
      <div className="row" style={{marginBottom:12}}>
        <div className="col card" style={{padding:14}}>
          <ProjectionChart product="Product U" traces={productU} />
          <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, marginTop:10}}>
            <TankCard name="U-Tank1" level={27446} capacity={35000} status="draw" />
            <TankCard name="U-Tank2" level={1000} capacity={35000} status="standby" />
          </div>
        </div>
        <div className="col card" style={{padding:14}}>
          <ProjectionChart product="Product M" traces={productM} />
          <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, marginTop:10}}>
            <TankCard name="M-Tank1" level={30446} capacity={35000} status="draw" />
            <TankCard name="M-Tank2" level={1000} capacity={35000} status="standby" />
          </div>
        </div>
      </div>

      {/* ── Forecast caption ── */}
      <div className="card" style={{display:'flex', justifyContent:'space-between', alignItems:'center', padding:'10px 16px', marginBottom:24}}>
        <div>
          <span className="eyebrow">🔮 Next-week forecast</span>{' '}
          <span className="secondary mono" style={{fontSize:'0.85rem', marginLeft:8}}>Mon 16h · Tue 16h · Wed 8h · Fri 8h</span>
        </div>
        <button className="btn btn-ghost btn-sm">↻ Refresh</button>
      </div>

      {/* ── Schedule + Planner ── */}
      <div className="row" style={{marginBottom:24}}>
        <div className="col" style={{flex:'2 1 0'}}><SchedulePanel /></div>
        <div className="col" style={{flex:'1 1 0'}}><PlannerPanel /></div>
      </div>

      {/* ── Health KPIs ── */}
      <SectionH3>📊 VMI Health Dashboard</SectionH3>
      <div className="row" style={{marginBottom:24}}>
        <Kpi label="Overfill alerts (6mo)" value="0" delta="↓ 2 vs prior period" />
        <Kpi label="Safety-stock alerts" value="3" delta="↑ 1 vs prior period" />
        <Kpi label="Alert bias" value="−0.4" delta="slightly under-warning" />
      </div>

      {/* ── Controls + Trucks ── */}
      <div className="row">
        <div className="col"><ControlsPanel /></div>
        <div className="col">
          <div className="card">
            <SectionH3 right={<Chip kind="receiving">3 SCHEDULED</Chip>}>🚛 Trucks</SectionH3>
            <table className="tbl">
              <thead><tr><th>SAP</th><th>Product</th><th>Qty</th><th>Arrival</th><th></th></tr></thead>
              <tbody>
                <tr><td className="num"><b>SAP20001</b></td><td>Product U</td><td className="num">33,000</td><td className="num">Tue 13:00</td><td><Chip kind="warning">RED FLAG</Chip></td></tr>
                <tr><td className="num"><b>SAP20002</b></td><td>Product M</td><td className="num">37,000</td><td className="num">Wed 10:00</td><td><Chip kind="warning">RED FLAG</Chip></td></tr>
                <tr><td className="num"><b>SAP20003</b></td><td>Product U</td><td className="num">35,000</td><td className="num">Fri 08:00</td><td><Chip kind="receiving">OK</Chip></td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [view, setView] = useState('dashboard');
  return view === 'roster'
    ? <Roster onOpen={() => setView('dashboard')} />
    : <Dashboard onBack={() => setView('roster')} />;
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
