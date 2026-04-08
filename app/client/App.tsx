import React from "react";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { fetchActiveIncidents } from "./api";
import ShiftHandover from "./components/ShiftHandover";
import MapView from "./components/MapView";
import AlarmLog from "./components/AlarmLog";
import RegulatoryView from "./components/RegulatoryView";
import GeniePage from "./pages/GeniePage";

function NetworkNormal() {
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] text-center px-4">
      <div className="w-20 h-20 rounded-full bg-rag-green/20 flex items-center justify-center mb-6">
        <div className="w-12 h-12 rounded-full bg-rag-green animate-pulse" />
      </div>
      <h2 className="text-2xl font-bold text-gray-800 mb-2">Network Normal</h2>
      <p className="text-gray-500 max-w-md">
        All District Metered Areas are operating within normal parameters. No active incidents detected.
      </p>
    </div>
  );
}

function NavBar({ hasIncident }: { hasIncident: boolean }) {
  const linkClass = ({ isActive }: { isActive: boolean }) =>
    `px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
      isActive
        ? "bg-water-700 text-white shadow-sm"
        : "text-gray-600 hover:bg-gray-100 hover:text-gray-900"
    }`;

  return (
    <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
      <div className="max-w-screen-2xl mx-auto px-4 h-14 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-water-700 flex items-center justify-center">
            <span className="text-white font-bold text-sm">W</span>
          </div>
          <span className="font-semibold text-water-800 text-lg">Water Utilities</span>
          <span className="text-xs text-gray-400 ml-1">Digital Twin</span>
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
      </div>
    </header>
  );
}

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
      <div className="min-h-screen flex flex-col">
        <NavBar hasIncident={hasIncident} />
        {hasIncident && (
          <div className="bg-red-50 border-b border-red-200 px-4 py-2">
            <div className="max-w-screen-2xl mx-auto flex items-center gap-2 text-sm">
              <span className="inline-block w-2 h-2 rounded-full bg-red-500 animate-pulse" />
              <span className="font-medium text-red-800">
                Active Incident: {activeIncident?.incident_id}
              </span>
              <span className="text-red-600">
                — {activeIncident?.description || activeIncident?.incident_type}
              </span>
            </div>
          </div>
        )}
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
