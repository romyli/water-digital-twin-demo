import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchActiveIncidents, fetchIncidentEvents, fetchRecentEvents } from "../api";
import { format } from "date-fns";
import { humanize, relativeTimeShort } from "../utils/format";
import RAGBadge from "./common/RAGBadge";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

export default function AlarmLog(_props: { activeIncident: any }) {
  const [selectedIncidentId, setSelectedIncidentId] = useState("all");
  const [severityFilter, setSeverityFilter] = useState("ALL");
  const [expandedRow, setExpandedRow] = useState<number | null>(null);

  const { data: incidentsData } = useQuery({
    queryKey: ["activeIncidents"],
    queryFn: fetchActiveIncidents,
  });

  const incidents = incidentsData?.incidents || [];
  const isAllMode = selectedIncidentId === "all";

  const { data: recentEventsData, isLoading: loadingRecent } = useQuery({
    queryKey: ["recentEvents"],
    queryFn: () => fetchRecentEvents(),
    enabled: isAllMode,
  });

  const { data: incidentEventsData, isLoading: loadingIncident } = useQuery({
    queryKey: ["incidentEvents", selectedIncidentId],
    queryFn: () => fetchIncidentEvents(selectedIncidentId),
    enabled: !isAllMode,
  });

  const eventsData = isAllMode ? recentEventsData : incidentEventsData;
  const isLoading = isAllMode ? loadingRecent : loadingIncident;
  const allEvents = eventsData?.events || [];

  const eventTypeColor = (type?: string) => {
    const t = type?.toLowerCase() || "";
    if (t.includes("alarm") || t.includes("alert")) return "RED";
    if (t.includes("warn") || t.includes("change")) return "AMBER";
    return "GREEN";
  };

  // Count by severity
  const counts = useMemo(() => {
    const c = { ALL: allEvents.length, RED: 0, AMBER: 0, GREEN: 0 };
    allEvents.forEach((e: any) => {
      const sev = eventTypeColor(e.event_type);
      c[sev as keyof typeof c]++;
    });
    return c;
  }, [allEvents]);

  const filteredEvents = useMemo(() => {
    if (severityFilter === "ALL") return allEvents;
    return allEvents.filter((e: any) => eventTypeColor(e.event_type) === severityFilter);
  }, [allEvents, severityFilter]);

  const severityRowClass = (type?: string) => {
    const sev = eventTypeColor(type);
    if (sev === "RED") return "bg-red-50/50 border-l-[3px] border-l-red-500";
    if (sev === "AMBER") return "bg-amber-50/30 border-l-[3px] border-l-amber-500";
    return "border-l-[3px] border-l-transparent";
  };

  const filterButtons = [
    { key: "ALL", label: "All", color: "bg-water-700 text-white" },
    { key: "RED", label: "Red", color: "bg-red-600 text-white" },
    { key: "AMBER", label: "Amber", color: "bg-amber-500 text-white" },
    { key: "GREEN", label: "Green", color: "bg-green-600 text-white" },
  ];

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold text-water-800">Alarm & Event Log</h1>
          <span className="text-sm text-gray-400 tabular-nums">{counts.ALL} events</span>
        </div>
        <select
          className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white"
          value={selectedIncidentId}
          onChange={(e) => setSelectedIncidentId(e.target.value)}
        >
          <option value="all">All events (last 24h)</option>
          {incidents.map((inc: any) => (
            <option key={inc.incident_id} value={inc.incident_id}>
              {inc.incident_id} — {inc.incident_type}
            </option>
          ))}
        </select>
      </div>

      {/* Segmented filter bar */}
      <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1 w-fit">
        {filterButtons.map((b) => (
          <button
            key={b.key}
            onClick={() => setSeverityFilter(b.key)}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              severityFilter === b.key
                ? b.color + " shadow-sm"
                : "text-gray-600 hover:text-gray-900"
            }`}
          >
            {b.label} ({counts[b.key as keyof typeof counts]})
          </button>
        ))}
      </div>

      {isLoading ? (
        <LoadingSpinner message="Loading events..." />
      ) : filteredEvents.length === 0 ? (
        <EmptyState title="No events" message="No alarm or event records found for the selected filters." />
      ) : (
        <div className="card overflow-hidden p-0">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200 sticky top-0 z-10">
                <th className="text-left px-4 py-3 font-medium text-gray-500">Time</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Type</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Source</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">Description</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {filteredEvents.map((evt: any, i: number) => {
                const ts = evt.event_timestamp ? new Date(evt.event_timestamp) : null;
                const relTime = ts ? relativeTimeShort(ts) : null;
                const absTime = ts ? format(ts, "dd MMM HH:mm:ss") : null;
                const isExpanded = expandedRow === i;
                return (
                  <tr
                    key={i}
                    onClick={() => setExpandedRow(isExpanded ? null : i)}
                    className={`cursor-pointer transition-colors hover:bg-gray-50 ${severityRowClass(evt.event_type)}`}
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <span className="text-gray-700 text-xs font-medium">{relTime || "—"}</span>
                      {absTime && (
                        <span className="text-gray-400 text-xs ml-1 hidden sm:inline">({absTime})</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <RAGBadge status={eventTypeColor(evt.event_type)} label={humanize(evt.event_type) || "—"} />
                    </td>
                    <td className="px-4 py-3 text-gray-600">{evt.actor || "—"}</td>
                    <td className="px-4 py-3 text-gray-700">
                      <span>{evt.description || "—"}</span>
                      {isExpanded && (
                        <div className="mt-2 p-2 bg-gray-50 rounded text-xs text-gray-500 space-y-1">
                          {evt.incident_id && <p>Incident: {evt.incident_id}</p>}
                          {evt.dma_code && <p>DMA: {evt.dma_code}</p>}
                          {evt.sensor_id && <p>Sensor: {evt.sensor_id}</p>}
                          {absTime && <p>Exact time: {absTime}</p>}
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
