const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

async function request(path, options = {}) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}${path}`, options);
  } catch {
    throw new Error(
      "The backend API could not be reached. Please confirm that FastAPI is running at "
      + `${API_BASE_URL}.`,
    );
  }
  if (!response.ok) {
    let detail = `Request failed with status ${response.status}`;
    try {
      const payload = await response.json();
      if (Array.isArray(payload.detail)) {
        detail = payload.detail
          .map((item) => {
            const location = Array.isArray(item.loc) ? item.loc.join(".") : "request";
            return `${location}: ${item.msg || "Invalid value"}`;
          })
          .join(" ");
      } else if (typeof payload.detail === "string") {
        detail = payload.detail;
      } else if (payload.detail) {
        detail = JSON.stringify(payload.detail);
      }
    } catch {
      // Preserve the HTTP status when the server did not return JSON.
    }
    throw new Error(detail);
  }
  return response.json();
}

export const api = {
  getSummary: () => request("/network/summary"),
  getGridActivity: (gridId) => request(`/network/grid/${gridId}`),
  getHotspots: (limit = 10, severity = "") =>
    request(`/network/hotspots?limit=${limit}${severity ? `&severity=${severity}` : ""}`),
  getAlerts: (limit = 10, severity = "") =>
    request(`/network/alerts?limit=${limit}${severity ? `&severity=${severity}` : ""}`),
  predictRisk: (gridId) =>
    request("/network/predict-risk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ grid_id: Number(gridId) }),
    }),
};

export { API_BASE_URL };
