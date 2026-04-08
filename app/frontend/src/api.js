/**
 * API client for Water Utilities Digital Twin backend.
 * Uses fetch() with the backend base URL.
 */

const BASE_URL = process.env.REACT_APP_API_URL || "";

async function request(path, options = {}) {
  const url = `${BASE_URL}${path}`;
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body || res.statusText}`);
  }
  if (res.headers.get("content-type")?.includes("application/pdf")) {
    return res.blob();
  }
  return res.json();
}

// --- Health ---
export const fetchHealth = () => request("/api/health");

// --- Incidents ---
export const fetchActiveIncidents = () => request("/api/incidents/active");
export const fetchIncident = (id) => request(`/api/incidents/${id}`);
export const fetchIncidentEvents = (id) => request(`/api/incidents/${id}/events`);
export const fetchHandover = (id) => request(`/api/incidents/${id}/handover`);

// --- DMA ---
export const fetchDMAStatus = (ragFilter) => {
  const qs = ragFilter ? `?rag_status=${ragFilter}` : "";
  return request(`/api/dma/status${qs}`);
};
export const fetchDMAPolygons = (code) => request(`/api/dma/${code}/polygons`);
export const fetchDMADetail = (code) => request(`/api/dma/${code}/detail`);
export const fetchDMASensors = (code) => request(`/api/dma/${code}/sensors`);
export const fetchDMAProperties = (code) => request(`/api/dma/${code}/properties`);
export const fetchDMAComplaints = (code) => request(`/api/dma/${code}/complaints`);
export const fetchDMAAssets = (code) => request(`/api/dma/${code}/assets`);
export const fetchDMARagHistory = (code, hours = 24) =>
  request(`/api/dma/${code}/rag-history?hours=${hours}`);

// --- Sensors ---
export const fetchSensorTelemetry = (id, hours = 24) =>
  request(`/api/sensor/${id}/telemetry?hours=${hours}`);
export const fetchSensorAnomalies = (id) => request(`/api/sensor/${id}/anomalies`);

// --- Map ---
export const fetchMapGeoJSON = () => request("/api/map/geojson");
export const fetchMapAssets = (dmaCode) => request(`/api/map/assets/${dmaCode}`);

// --- Playbooks ---
export const fetchPlaybook = (type) => request(`/api/playbooks/${type}`);

// --- Reservoirs ---
export const fetchReservoirs = (dmaCode) => request(`/api/reservoirs/${dmaCode}`);

// --- Communications ---
export const fetchComms = (incidentId) => request(`/api/comms/${incidentId}`);
export const addComms = (incidentId, entry) =>
  request(`/api/comms/${incidentId}`, { method: "POST", body: JSON.stringify(entry) });

// --- Playbook Actions ---
export const savePlaybookActions = (incidentId, actions) =>
  request(`/api/playbook-actions/${incidentId}`, {
    method: "POST",
    body: JSON.stringify(actions),
  });

// --- Proactive Comms Request ---
export const createCommsRequest = (incidentId, req) =>
  request(`/api/comms-request/${incidentId}`, {
    method: "POST",
    body: JSON.stringify(req),
  });

// --- PDF Report ---
export const generatePDFReport = (incidentId) =>
  request(`/api/incidents/${incidentId}/report/pdf`, { method: "POST" });

// --- Regulatory ---
export const fetchRegulatory = (incidentId) => request(`/api/regulatory/${incidentId}`);
