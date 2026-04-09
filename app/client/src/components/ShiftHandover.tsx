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

export default function ShiftHandover({ incident }: { incident: any }) {
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
      setAckTime(new Date());
      queryClient.invalidateQueries({ queryKey: ["handover", incident.incident_id] });
    },
  });

  const { elapsed } = useCountdown(incident?.created_at || incident?.start_timestamp);

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

  // Build timeline nodes
  const timelineNodes: TimelineNode[] = [
    ...actionsTaken.map((a: any) => ({
      label: a.event_type || "Action",
      description: a.description,
      timestamp: a.event_timestamp,
      status: "done" as const,
    })),
    ...outstanding.map((a: any) => ({
      label: a.event_type || "Action",
      description: a.description,
      timestamp: a.event_timestamp,
      status: "outstanding" as const,
    })),
  ];

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
      <div className="grid grid-cols-4 gap-4">
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
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 border-l-4 border-l-purple-500">
          <p className="text-xs text-gray-500">Escalation Risk</p>
          <p className="text-sm font-semibold text-gray-900">
            {inc.escalation_risk || "Monitoring"}
          </p>
        </div>
      </div>

      {/* Actions Timeline */}
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
