import { useEffect, useMemo, useState } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { api } from "./api";
import { ErrorState, Loading } from "./re1_shell";

export default function Hotspots({ onNavigate }) {
  const [severity, setSeverity] = useState("");
  const [data, setData] = useState({ hotspots: null, alerts: null });
  const [geojson, setGeojson] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch("/reference/milano-grid.geojson")
      .then((response) => {
        if (!response.ok) throw new Error(`Map request failed with status ${response.status}`);
        return response.json();
      })
      .then(setGeojson)
      .catch((requestError) => setError(requestError.message));
  }, []);

  useEffect(() => {
    setData({ hotspots: null, alerts: null });
    Promise.all([api.getHotspots(10, severity), api.getAlerts(10, severity)])
      .then(([hotspots, alerts]) => setData({ hotspots, alerts }))
      .catch((requestError) => setError(requestError.message));
  }, [severity]);

  const selectedIds = useMemo(() => {
    if (!data.hotspots) return new Set();
    return new Set(data.hotspots.items.map((item) => item.grid_id));
  }, [data.hotspots]);

  if (error) return <ErrorState message={error} />;
  if (!data.hotspots || !data.alerts) return <Loading label="Loading hotspots, alerts and Milan grid..." />;

  return (
    <>
      <section className="hero compact"><div><p className="eyebrow">RE4 · OPERATIONAL ATTENTION</p><h2>Hotspots & alerts</h2><p className="muted">GeoJSON is loaded once; the full grid is shown with API-selected hotspots highlighted.</p></div>
        <label className="filter-label">Severity<select value={severity} onChange={(event) => setSeverity(event.target.value)}><option value="">All</option><option value="HIGH">High</option><option value="ATTENTION">Attention</option></select></label>
      </section>
      <section className="map-layout"><div className="panel"><div className="panel-heading"><h3>Ranked hotspots</h3><span>AS_OF {data.hotspots.as_of}</span></div>
        {data.hotspots.items.map((item) => <button className="ranked-row" key={`${item.grid_id}-${item.timestamp}`} onClick={() => onNavigate("grid")}><span className="severity high">HIGH</span><strong>Grid {item.grid_id}</strong><span>{item.total_activity.toFixed(2)}</span><small>{item.timestamp}</small></button>)}
        <h3 className="subheading">Alerts</h3>{data.alerts.items.map((item) => <div className="alert-row" key={`${item.grid_id}-${item.timestamp}-${item.alert_type}`}><span className={`severity ${item.severity.toLowerCase()}`}>{item.severity}</span><strong>Grid {item.grid_id}</strong><span>{item.alert_type}</span></div>)}
      </div><PolygonMap features={geojson?.features || []} selectedIds={selectedIds} /></section>
    </>
  );
}

function PolygonMap({ features, selectedIds }) {
  const mapContainerRef = useMemo(() => ({ current: null }), []);

  useEffect(() => {
    if (!mapContainerRef.current || !features.length) return undefined;
    const map = L.map(mapContainerRef.current, { zoomControl: true });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 19,
    }).addTo(map);
    const geoJsonLayer = L.geoJSON(features, {
      style: (feature) => ({
        color: selectedIds.has(Number(feature.properties.cellId)) ? "#ffcf70" : "#4e7295",
        weight: selectedIds.has(Number(feature.properties.cellId)) ? 2 : 0.5,
        fillColor: selectedIds.has(Number(feature.properties.cellId)) ? "#f0a34a" : "#345473",
        fillOpacity: selectedIds.has(Number(feature.properties.cellId)) ? 0.75 : 0.18,
      }),
      onEachFeature: (feature, layer) => {
        layer.bindTooltip(`Grid ${feature.properties.cellId}`, { sticky: true });
      },
    }).addTo(map);
    map.fitBounds(geoJsonLayer.getBounds(), { padding: [18, 18] });
    const selectedLayer = L.layerGroup().addTo(map);
    geoJsonLayer.eachLayer((layer) => {
      const feature = layer.feature;
      if (selectedIds.has(Number(feature.properties.cellId))) {
        const center = layer.getBounds().getCenter();
        L.circleMarker(center, {
          radius: 7,
          color: "#ffffff",
          weight: 2,
          fillColor: "#e5484d",
          fillOpacity: 1,
        }).bindTooltip(`Hotspot: Grid ${feature.properties.cellId}`).addTo(selectedLayer);
      }
    });
    return () => map.remove();
  }, [features, selectedIds, mapContainerRef]);

  return <div className="map-panel"><h3>Milan network map</h3><div ref={(element) => { mapContainerRef.current = element; }} className="leaflet-map" role="img" aria-label="Milan basemap with network grid and hotspots" /><p className="muted">OpenStreetMap basemap with grid overlay joined by <code>properties.cellId</code>; hotspots are marked in red.</p></div>;
}
