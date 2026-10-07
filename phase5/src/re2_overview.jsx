import { useEffect, useState } from "react";
import { api } from "./api";
import { ErrorState, Loading } from "./re1_shell";

export default function Overview() {
  const [state, setState] = useState({ loading: true, data: null, error: null });

  useEffect(() => {
    api.getSummary()
      .then((data) => setState({ loading: false, data, error: null }))
      .catch((error) => setState({ loading: false, data: null, error: error.message }));
  }, []);

  if (state.loading) return <Loading />;
  if (state.error) return <ErrorState message={state.error} />;

  const { data } = state;
  return (
    <>
      <section className="hero">
        <div>
          <p className="eyebrow">NOC OVERVIEW</p>
          <h2>Current network activity</h2>
          <p className="muted">Reporting timestamp: <strong>{data.as_of}</strong></p>
        </div>
        <span className="status-pill">API CONNECTED</span>
      </section>
      <section className="metric-grid">
        <Metric label="Total activity indicator" value={data.total_activity.toLocaleString()} />
        <Metric label="Active grids" value={data.active_grids.toLocaleString()} />
        <Metric label="Peak hour" value={`${String(data.peak_hour).padStart(2, "0")}:00`} />
        <Metric label="Top grid" value={data.top_grid} />
      </section>
    </>
  );
}

function Metric({ label, value }) {
  return <article className="metric-card"><span>{label}</span><strong>{value}</strong></article>;
}
