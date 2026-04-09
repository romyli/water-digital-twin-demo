import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { fetchHandover, addComms } from "../api";
import { format } from "date-fns";
import { useCountdown } from "../hooks/useCountdown";
import RAGBadge from "./common/RAGBadge";
import Timeline, { type TimelineNode } from "./common/Timeline";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";
import CommunicationsLog from "./CommunicationsLog";

export default function ShiftHandover({ incident, timeOffset = 0 }: { incident: any; timeOffset?: number }) {
  const [acknowledged, setAcknowledged] = useState(false);
  const [ackTime, setAckTime] = useState<Date | null>(null);
  const queryClient = useQueryClient();

  const { data, isLoading, error } = useQuery({
    queryKey: ["handover", incident?.incident_id],
    queryFn: () => fetchHandover(incident.incident_id),
    enabled: !!incident?.incident_id,
  });

  const ackMutation = useMutation({
    mutationFn: () =>
      addComms(incident.incident_id, {
        channel: "system",
        message: "Shift handover acknowledged by incoming operator",
        sent_by: "operator",
      }),
    onSuccess: () => {
      setAcknowledged(true);
      setAckTime(new Date(timeOffset ? Date.now() - timeOffset : Date.now()));
      queryClient.invalidateQueries({ queryKey: ["handover", incident.incident_id] });
    },
  });

  const { elapsed } = useCountdown(incident?.created_at || incident?.start_timestamp, timeOffset);

  if (isLoading) return <LoadingSpinner message="Loading shift handover..." />;
  if (error)
    return (
      <div className="max-w-5xl mx-auto p-6">
        <div className="card text-red-600">Failed to load handover: {(error as Error).message}</div>
      </div>
    );

  const handover = data || {};
  const inc = handover.incident || incident || {};
  const actionsTaken = handover.actions_taken || [];
  const outstanding = handover.outstanding_actions || [];
  const comms = handover.communications || [];

  const createdAt = inc.created_at ? new Date(inc.created_at) : null;

  const anomalyLeadMinutes = inc.anomaly_lead_minutes;

  // Build timeline nodes from completed actions
  const timelineNodes: TimelineNode[] = actionsTaken.map((a: any) => ({
    label: a.event_type || "Action",
    description: a.description,
    timestamp: a.event_timestamp,
    status: "done" as const,
  }));

  const PRIORITY_COLORS: Record<string, string> = {
    critical: "border-l-red-500 bg-red-50",
    high: "border-l-amber-500 bg-amber-50",
    medium: "border-l-yellow-400 bg-yellow-50",
    low: "border-l-gray-300 bg-gray-50",
  };
  const PRIORITY_BADGE: Record<string, string> = {
    critical: "bg-red-100 text-red-800",
    high: "bg-amber-100 text-amber-800",
    medium: "bg-yellow-100 text-yellow-800",
    low: "bg-gray-100 text-gray-600",
  };

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-5 pb-24">
      {/* Hero Section */}
      <div className="card-elevated">
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <RAGBadge status="RED" label="Active Incident" pulse />
              <span className="text-xs text-gray-400">
                {inc.incident_id}
              </span>
            </div>
            <h1 className="text-xl font-bold text-water-800 mb-1">
              {inc.incident_type || "Incident"} — {inc.dma_code || "Network"}
            </h1>
            <p className="text-sm text-gray-500">
              {inc.description || "Active incident requiring attention"}
            </p>
            {createdAt && (
              <p className="text-xs text-gray-400 mt-1">
                Started {format(createdAt, "dd MMM yyyy HH:mm")}
              </p>
            )}
          </div>
          <div className="text-right">
            <p className="font-mono text-4xl font-bold text-water-800 tabular-nums">{elapsed}</p>
            <p className="text-xs text-gray-400">elapsed</p>
          </div>
        </div>
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-5 gap-4">
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 border-l-4 border-l-red-500">
          <p className="text-xs text-gray-500">Duration</p>
          <p className="kpi-display text-xl text-gray-900">{elapsed}</p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 border-l-4 border-l-amber-500">
          <p className="text-xs text-gray-500">Affected DMAs</p>
          <p className="kpi-display text-xl text-gray-900">
            {inc.affected_dma_count ?? inc.dma_count ?? "—"}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 border-l-4 border-l-blue-500">
          <p className="text-xs text-gray-500">Properties at Risk</p>
          <p className="kpi-display text-xl text-gray-900">
            {inc.affected_properties ?? inc.property_count ?? "—"}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 border-l-4 border-l-emerald-500">
          <p className="text-xs text-gray-500">Anomaly Lead Time</p>
          <p className="kpi-display text-xl text-gray-900">
            {anomalyLeadMinutes != null ? `${anomalyLeadMinutes} min` : "—"}
          </p>
          {anomalyLeadMinutes != null && (
            <p className="text-[10px] text-gray-400">Detection → 1st complaint</p>
          )}
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 border-l-4 border-l-purple-500">
          <p className="text-xs text-gray-500">Escalation Risk</p>
          <p className="text-sm font-semibold text-gray-900">
            {inc.escalation_risk || "Monitoring"}
          </p>
        </div>
      </div>

      {/* Outstanding Actions */}
      {outstanding.length > 0 && (
        <div className="card">
          <h2 className="panel-title">Outstanding Actions ({outstanding.length})</h2>
          <div className="space-y-2">
            {outstanding.map((a: any) => (
              <div
                key={a.action_id}
                className={`border-l-4 rounded-lg p-3 ${PRIORITY_COLORS[a.priority] || PRIORITY_COLORS.low}`}
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-gray-900">{a.action_description}</p>
                    <div className="flex items-center gap-2 mt-1 flex-wrap">
                      <span className={`text-[10px] font-bold uppercase px-1.5 py-0.5 rounded ${PRIORITY_BADGE[a.priority] || PRIORITY_BADGE.low}`}>
                        {a.priority}
                      </span>
                      {a.assigned_role && (
                        <span className="text-xs text-gray-500">{a.assigned_role}</span>
                      )}
                      {a.due_by && (
                        <span className="text-xs text-gray-400">
                          Due: {format(new Date(a.due_by), "HH:mm")}
                        </span>
                      )}
                    </div>
                    {a.notes && (
                      <p className="text-xs text-gray-500 mt-1">{a.notes}</p>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Completed Actions Timeline */}
      <div className="card">
        <h2 className="panel-title">Actions Timeline</h2>
        {timelineNodes.length > 0 ? (
          <Timeline nodes={timelineNodes} />
        ) : (
          <EmptyState title="No actions recorded" message="No actions recorded for this incident yet." />
        )}
      </div>

      {/* Current Trajectory */}
      <div className="card">
        <h2 className="panel-title">Current Trajectory & Escalation Risk</h2>
        <div className="text-sm text-gray-600 space-y-2">
          <p>
            <span className="font-medium">Escalation Risk:</span>{" "}
            {inc.escalation_risk || "Monitoring — assess at next review point"}
          </p>
          <p>
            <span className="font-medium">Trend:</span>{" "}
            {inc.trajectory || "Pressure stabilising but below normal. Recovery dependent on upstream supply restoration."}
          </p>
        </div>
      </div>

      {/* Communications */}
      <CommunicationsLog incidentId={incident?.incident_id} initialComms={comms} />

      {/* Sticky Acknowledge Footer */}
      <div
        className={`fixed bottom-0 left-0 right-0 z-40 px-4 py-3 border-t-2 ${
          acknowledged
            ? "bg-green-50 border-green-400"
            : "bg-amber-50 border-amber-400"
        }`}
      >
        <div className="max-w-5xl mx-auto flex items-center justify-between">
          <div>
            <h2 className="font-semibold text-gray-800 text-sm">Acknowledge Handover</h2>
            <p className="text-xs text-gray-500">
              {acknowledged && ackTime
                ? `Acknowledged at ${format(ackTime, "HH:mm:ss")}`
                : "Confirm you have reviewed this handover briefing."}
            </p>
          </div>
          {acknowledged ? (
            <div className="flex items-center gap-2 text-green-700 font-medium text-sm">
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
              </svg>
              Acknowledged
            </div>
          ) : (
            <button
              className="px-5 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg font-medium text-sm transition-colors"
              onClick={() => ackMutation.mutate()}
              disabled={ackMutation.isPending}
            >
              {ackMutation.isPending ? "Saving..." : "Acknowledge"}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
