import { useState } from "react";
import { api } from "./api";
import { ErrorState, Loading } from "./re1_shell";

export default function GridExplorer() {
  const [gridId, setGridId] = useState("4821");
  const [state, setState] = useState({ loading: false, data: null, error: null });

  async function loadGrid(event) {
    event.preventDefault();
    setState({ loading: true, data: null, error: null });
    try {
      setState({ loading: false, data: await api.getGridActivity(gridId), error: null });
    } catch (error) {
      setState({ loading: false, data: null, error: error.message });
    }
  }

  return (
    <>
      <section className="hero compact">
        <div><p className="eyebrow">RE3 · GRID EXPLORER</p><h2>Hourly activity drill-down</h2></div>
        <form className="inline-form" onSubmit={loadGrid}>
          <label htmlFor="grid-id">Grid ID</label>
          <input id="grid-id" value={gridId} onChange={(event) => setGridId(event.target.value)} inputMode="numeric" />
          <button className="primary-button" type="submit">Load grid</button>
        </form>
      </section>
      {state.loading && <Loading />}
      {state.error && <ErrorState message={state.error} />}
      {state.data && <ActivityTable data={state.data} />}
    </>
  );
}

function ActivityTable({ data }) {
  return (
    <section className="panel">
      <div className="panel-heading"><h3>Grid {data.grid_id}</h3><span>AS_OF {data.as_of}</span></div>
      <div className="table-scroll"><table><thead><tr><th>Timestamp</th><th>SMS</th><th>Calls</th><th>Internet</th><th>Total activity</th></tr></thead>
        <tbody>{data.points.map((point) => <tr key={point.timestamp}><td>{point.timestamp}</td><td>{point.total_sms.toFixed(2)}</td><td>{point.total_calls.toFixed(2)}</td><td>{point.internet_activity.toFixed(2)}</td><td>{point.total_activity.toFixed(2)}</td></tr>)}</tbody>
      </table></div>
    </section>
  );
}
