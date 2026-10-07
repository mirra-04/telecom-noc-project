import { useState } from "react";
import { api } from "./api";
import { ErrorState, Loading } from "./re1_shell";

export default function RiskView() {
  const [gridId, setGridId] = useState("4821");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function submit(event) {
    event.preventDefault();
    setError(null);
    setResult(null);
    setLoading(true);
    try { setResult(await api.predictRisk(gridId)); } catch (requestError) { setResult(null); setError(requestError.message); }
    finally { setLoading(false); }
  }

  return <><section className="hero compact"><div><p className="eyebrow">RE5 · PREDICTIVE RISK</p><h2>Model output</h2><p className="muted">Prediction and explanation remain separate for later AI integration.</p></div></section>
    <section className="panel risk-panel"><form className="inline-form" onSubmit={submit}><label htmlFor="risk-grid">Grid ID</label><input id="risk-grid" value={gridId} onChange={(event) => setGridId(event.target.value)} disabled={loading} /><button className="primary-button" type="submit" disabled={loading}>{loading ? "Loading..." : "Request risk"}</button></form>
      {loading && <Loading label="Requesting risk prediction..." />}
      {error && <ErrorState title="Risk prediction unavailable" message={error} />}{result && <div className="risk-result"><div><span className="muted">Risk score</span><strong>{result.risk_score}</strong></div><div><span className="muted">Risk level</span><strong>{result.risk_level}</strong></div><div><span className="muted">Model version</span><strong>{result.model_version}</strong></div><p>{result.explanation_note}</p><button className="secondary-button" disabled>Explain with AI</button></div>}
    </section></>;
}
