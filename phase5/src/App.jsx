import { useState } from "react";
import Overview from "./re2_overview";
import GridExplorer from "./re3_grid_explorer";
import Hotspots from "./re4_hotspots";
import RiskView from "./re5_risk";

const pages = {
  overview: { label: "Overview", component: Overview },
  grid: { label: "Grid Explorer", component: GridExplorer },
  hotspots: { label: "Hotspots & Alerts", component: Hotspots },
  risk: { label: "Predictive Risk", component: RiskView },
};

export default function App() {
  const [page, setPage] = useState("overview");
  const Page = pages[page].component;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">MILAN NETWORK OPERATIONS</p>
          <h1>Network Intelligence</h1>
        </div>
        <nav aria-label="Primary navigation">
          {Object.entries(pages).map(([key, item]) => (
            <button
              className={page === key ? "nav-button active" : "nav-button"}
              key={key}
              onClick={() => setPage(key)}
            >
              {item.label}
            </button>
          ))}
        </nav>
      </header>
      <main className="page-content">
        <Page onNavigate={setPage} />
      </main>
    </div>
  );
}
