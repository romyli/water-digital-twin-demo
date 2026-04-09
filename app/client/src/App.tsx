import { useState, useEffect, useCallback } from "react";
import { BrowserRouter, Routes, Route, NavLink, useNavigate, useLocation } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { fetchActiveIncidents, fetchHealth } from "./api";
import { useCountdown } from "./hooks/useCountdown";
import { humanize } from "./utils/format";
import RAGBadge from "./components/common/RAGBadge";
import ShiftHandover from "./components/ShiftHandover";
import MapView from "./components/MapView";
import AlarmLog from "./components/AlarmLog";
import RegulatoryView from "./components/RegulatoryView";
import GeniePage from "./pages/GeniePage";

function NetworkNormal() {
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] text-center px-4">
      <div className="w-20 h-20 rounded-full bg-rag-green/20 flex items-center justify-center mb-6">
        <div className="w-12 h-12 rounded-full bg-rag-green" />
      </div>
      <h2 className="text-2xl font-bold text-gray-800 mb-2">Network Normal</h2>
      <p className="text-gray-500 max-w-md">
        All District Metered Areas are operating within normal parameters. No active incidents detected.
      </p>
    </div>
  );
}

/* ---------- Live clock (HH:mm:ss) ---------- */
function LiveClock() {
  const [time, setTime] = useState(() => new Date().toLocaleTimeString("en-GB", { hour12: false }));
  useEffect(() => {
    const id = setInterval(
      () => setTime(new Date().toLocaleTimeString("en-GB", { hour12: false })),
      1000
    );
    return () => clearInterval(id);
  }, []);
  return <span className="font-mono text-sm tabular-nums text-white/80">{time}</span>;
}

/* ---------- Connection status ---------- */
function ConnectionStatus() {
  const { isError, dataUpdatedAt } = useQuery({
    queryKey: ["health"],
    queryFn: fetchHealth,
    refetchInterval: 15_000,
    retry: 1,
  });
  const stale = dataUpdatedAt > 0 && Date.now() - dataUpdatedAt > 60_000;
  if (isError || stale) {
    return (
      <span className="flex items-center gap-1.5 text-xs text-red-300">
        <span className="w-2 h-2 rounded-full bg-red-400" />
        DISCONNECTED
      </span>
    );
  }
  return (
    <span className="flex items-center gap-1.5 text-xs text-green-300">
      <span className="w-2 h-2 rounded-full bg-green-400" />
      LIVE
    </span>
  );
}

/* ---------- Incident Banner ---------- */
function IncidentBanner({ incident }: { incident: any }) {
  const { elapsed } = useCountdown(incident?.created_at || incident?.start_timestamp);
  const dmaCount = incident?.affected_dma_count ?? incident?.dma_count ?? "—";
  const propCount = incident?.affected_properties ?? incident?.property_count ?? "—";
  const sensitiveCount = incident?.sensitive_site_count ?? "—";

  return (
    <div className="bg-red-600 text-white px-4 py-1.5">
      <div className="max-w-screen-2xl mx-auto flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <span className="inline-block w-2 h-2 rounded-full bg-white animate-pulse flex-shrink-0" />
          <span className="font-semibold text-sm whitespace-nowrap">{incident.incident_id}</span>
          <RAGBadge status="RED" label={humanize(incident.incident_type) || "Active"} className="!bg-red-800/50 !text-white flex-shrink-0" />
          <span className="text-red-100 text-xs truncate hidden lg:inline">
            {incident.description || ""}
          </span>
        </div>
        <div className="flex items-center gap-5 flex-shrink-0">
          <div className="flex items-center gap-4 text-xs text-red-100">
            <span>{dmaCount} DMAs</span>
            <span>{propCount} properties</span>
            <span>{sensitiveCount} sensitive</span>
          </div>
          <span className="font-mono font-bold text-sm tabular-nums">{elapsed}</span>
        </div>
      </div>
    </div>
  );
}

/* ---------- Nav Bar ---------- */
function NavBar({ hasIncident }: { hasIncident: boolean }) {
  const linkClass = ({ isActive }: { isActive: boolean }) =>
    `px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
      isActive
        ? "bg-white/15 text-white"
        : "text-white/70 hover:text-white hover:bg-white/10"
    }`;

  return (
    <header className="bg-water-900 sticky top-0 z-50">
      <div className="max-w-screen-2xl mx-auto px-4 h-12 flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* Water droplet icon */}
          <svg className="w-6 h-6 text-blue-400" viewBox="0 0 24 24" fill="currentColor">
            <path d="M12 2c0 0-7 8.2-7 13a7 7 0 0 0 14 0C19 10.2 12 2 12 2z" />
          </svg>
          <span className="font-semibold text-white text-sm">Water Utilities</span>
          <span className="text-xs text-white/40">Digital Twin</span>
        </div>
        <nav className="flex items-center gap-1">
          <NavLink to="/" end className={linkClass}>
            Handover
          </NavLink>
          <NavLink to="/map" className={linkClass}>
            Network Map
          </NavLink>
          <NavLink to="/incidents" className={linkClass}>
            Alarm Log
          </NavLink>
          {hasIncident && (
            <NavLink to="/regulatory" className={linkClass}>
              Regulatory
            </NavLink>
          )}
          <NavLink to="/genie" className={linkClass}>
            Ask Genie
          </NavLink>
        </nav>
        <div className="flex items-center gap-4">
          <ConnectionStatus />
          <LiveClock />
        </div>
      </div>
    </header>
  );
}

/* ---------- Keyboard shortcuts ---------- */
function KeyboardShortcuts() {
  const navigate = useNavigate();
  const location = useLocation();

  const handler = useCallback(
    (e: KeyboardEvent) => {
      // Don't fire when typing in inputs
      if (
        e.target instanceof HTMLInputElement ||
        e.target instanceof HTMLTextAreaElement ||
        e.target instanceof HTMLSelectElement
      )
        return;

      const routes = ["/", "/map", "/incidents", "/regulatory", "/genie"];
      const key = e.key;
      if (key >= "1" && key <= "5") {
        e.preventDefault();
        const idx = Number(key) - 1;
        if (routes[idx] && routes[idx] !== location.pathname) {
          navigate(routes[idx]);
        }
      }
      if (key === "Escape") {
        // Escape is handled by individual components (e.g., close side panel)
        // Dispatch a custom event so components can listen
        window.dispatchEvent(new CustomEvent("app:escape"));
      }
    },
    [navigate, location.pathname]
  );

  useEffect(() => {
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [handler]);

  return null;
}

/* ---------- App ---------- */
export default function App() {
  const { data } = useQuery({
    queryKey: ["activeIncidents"],
    queryFn: fetchActiveIncidents,
    refetchInterval: 60_000,
  });

  const incidents = data?.incidents || [];
  const activeIncident = incidents[0] || null;
  const hasIncident = incidents.length > 0;

  return (
    <BrowserRouter>
      <KeyboardShortcuts />
      <div className="min-h-screen flex flex-col">
        <NavBar hasIncident={hasIncident} />
        {hasIncident && <IncidentBanner incident={activeIncident} />}
        <main className="flex-1">
          <Routes>
            <Route
              path="/"
              element={
                hasIncident ? <ShiftHandover incident={activeIncident} /> : <NetworkNormal />
              }
            />
            <Route path="/map" element={<MapView activeIncident={activeIncident} />} />
            <Route path="/incidents" element={<AlarmLog activeIncident={activeIncident} />} />
            <Route
              path="/regulatory"
              element={
                hasIncident ? <RegulatoryView incident={activeIncident} /> : <NetworkNormal />
              }
            />
            <Route path="/genie" element={<GeniePage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
