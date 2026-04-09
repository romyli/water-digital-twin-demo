/**
 * API client for Water Utilities Digital Twin backend.
 */

async function request(path: string, options: RequestInit = {}): Promise<any> {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body || res.statusText}`);
  }
  return res.json();
}

// --- Health ---
export const fetchHealth = () => request("/api/health");

// --- Incidents ---
export const fetchActiveIncidents = () => request("/api/incidents/active");
export const fetchIncident = (id: string) => request(`/api/incidents/${id}`);
export const fetchIncidentEvents = (id: string) => request(`/api/incidents/${id}/events`);
export const fetchRecentEvents = (hours = 24) => request(`/api/incidents/events/recent?hours=${hours}`);
export const fetchHandover = (id: string) => request(`/api/incidents/${id}/handover`);

// --- DMA ---
export const fetchDMAStatus = (ragFilter?: string) => {
  const qs = ragFilter ? `?rag_status=${ragFilter}` : "";
  return request(`/api/dma/status${qs}`);
};
export const fetchDMAPolygons = (code: string) => request(`/api/dma/${code}/polygons`);
export const fetchDMADetail = (code: string) => request(`/api/dma/${code}/detail`);
export const fetchDMASensors = (code: string) => request(`/api/dma/${code}/sensors`);
export const fetchDMAProperties = (code: string) => request(`/api/dma/${code}/properties`);
export const fetchDMAComplaints = (code: string) => request(`/api/dma/${code}/complaints`);
export const fetchDMAAssets = (code: string) => request(`/api/dma/${code}/assets`);
export const fetchDMARagHistory = (code: string, hours = 24) =>
  request(`/api/dma/${code}/rag-history?hours=${hours}`);

// --- Sensors ---
export const fetchSensorTelemetry = (id: string, hours = 24) =>
  request(`/api/sensor/${id}/telemetry?hours=${hours}`);
export const fetchSensorAnomalies = (id: string) => request(`/api/sensor/${id}/anomalies`);

// --- Map ---
export const fetchMapGeoJSON = () => request("/api/map/geojson");
export const fetchMapAssets = (dmaCode: string) => request(`/api/map/assets/${dmaCode}`);

// --- Playbooks ---
export const fetchPlaybook = (type: string) => request(`/api/playbooks/${type}`);

// --- Reservoirs ---
export const fetchReservoirs = (dmaCode: string) => request(`/api/reservoirs/${dmaCode}`);

// --- Communications ---
export const fetchComms = (incidentId: string) => request(`/api/comms/${incidentId}`);
export const addComms = (incidentId: string, entry: any) =>
  request(`/api/comms/${incidentId}`, { method: "POST", body: JSON.stringify(entry) });

// --- Playbook Actions ---
export const savePlaybookActions = (incidentId: string, actions: any[]) =>
  request(`/api/playbook-actions/${incidentId}`, {
    method: "POST",
    body: JSON.stringify(actions),
  });

// --- Proactive Comms Request ---
export const createCommsRequest = (incidentId: string, req: any) =>
  request(`/api/comms-request/${incidentId}`, {
    method: "POST",
    body: JSON.stringify(req),
  });

// --- Regulatory ---
export const fetchRegulatory = (incidentId: string) => request(`/api/regulatory/${incidentId}`);
