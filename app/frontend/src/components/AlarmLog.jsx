import React, { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchActiveIncidents, fetchIncidentEvents } from "../api";
import { format } from "date-fns";
import RAGBadge from "./common/RAGBadge";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

export default function AlarmLog({ activeIncident }) {
  const [selectedIncidentId, setSelectedIncidentId] = useState("all");
  const [typeFilter, setTypeFilter] = useState("all");

  const { data: incidentsData } = useQuery({
    queryKey: ["activeIncidents"],
    queryFn: fetchActiveIncidents,
  });

  const incidents = incidentsData?.incidents || [];

  // Fetch events for selected incident or the first active one
  const queryIncidentId =
    selectedIncidentId === "all"
      ? activeIncident?.incident_id
      : selectedIncidentId;

  const { data: eventsData, isLoading } = useQuery({
    queryKey: ["incidentEvents", queryIncidentId],
    queryFn: () => fetchIncidentEvents(queryIncidentId),
    enabled: !!queryIncidentId,
  });

  const allEvents = eventsData?.events || [];

  // Extract unique event types for filtering
  const eventTypes = useMemo(() => {
    const types = new Set(allEvents.map((e) => e.event_type).filter(Boolean));
    return Array.from(types).sort();
  }, [allEvents]);

  const filteredEvents = useMemo(() => {
    if (typeFilter === "all") return allEvents;
    return allEvents.filter((e) => e.event_type === typeFilter);
  }, [allEvents, typeFilter]);

  const eventTypeColor = (type) => {
    const t = type?.toLowerCase() || "";
    if (t.includes("alarm") || t.includes("alert")) return "RED";
    if (t.includes("warn") || t.includes("change")) return "AMBER";
    return "GREEN";
  };

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-water-800">Alarm & Event Log</h1>
        <div className="flex items-center gap-3">
          {/* Incident selector */}
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white"
            value={selectedIncidentId}
            onChange={(e) => setSelectedIncidentId(e.target.value)}
          >
            <option value="all">All events (last 24h)</option>
            {incidents.map((inc) => (
              <option key={inc.incident_id} value={inc.incident_id}>
                {inc.incident_id} — {inc.incident_type}
              </option>
            ))}
          </select>

          {/* Type filter */}
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
          >
            <option value="all">All types</option>
            {eventTypes.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>
      </div>

      {isLoading ? (
        <LoadingSpinner message="Loading events..." />
      ) : filteredEvents.length === 0 ? (
        <EmptyState
          title="No events"
          message="No alarm or event records found for the selected filters."
        />
      ) : (
        <div className="card overflow-hidden p-0">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left px-4 py-3 font-medium text-gray-500">Time</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Type</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Source</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Description</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {filteredEvents.map((evt, i) => (
                <tr key={i} className="hover:bg-gray-50 transition-colors">
                  <td className="px-4 py-3 text-gray-600 whitespace-nowrap">
                    {evt.event_time
                      ? format(new Date(evt.event_time), "dd MMM HH:mm:ss")
                      : "—"}
                  </td>
                  <td className="px-4 py-3">
                    <RAGBadge status={eventTypeColor(evt.event_type)} label={evt.event_type || "—"} />
                  </td>
                  <td className="px-4 py-3 text-gray-600">{evt.source || evt.sensor_id || "—"}</td>
                  <td className="px-4 py-3 text-gray-700">{evt.description || "—"}</td>
                  <td className="px-4 py-3">
                    <span
                      className={`text-xs font-medium capitalize ${
                        evt.status === "completed"
                          ? "text-green-600"
                          : evt.status === "pending"
                          ? "text-amber-600"
                          : "text-gray-500"
                      }`}
                    >
                      {evt.status || "—"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
