import React, { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { fetchHandover, addComms } from "../api";
import { format, formatDistanceToNow } from "date-fns";
import RAGBadge from "./common/RAGBadge";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";
import CommunicationsLog from "./CommunicationsLog";

export default function ShiftHandover({ incident }: { incident: any }) {
  const [acknowledged, setAcknowledged] = useState(false);
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
      queryClient.invalidateQueries({ queryKey: ["handover", incident.incident_id] });
    },
  });

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
  const elapsed = createdAt ? formatDistanceToNow(createdAt, { addSuffix: false }) : "N/A";

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-5">
      {/* Header */}
      <div className="card">
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-xl font-bold text-water-800 mb-1">Shift Handover</h1>
            <p className="text-sm text-gray-500">
              Incident {inc.incident_id} — {elapsed} elapsed
            </p>
          </div>
          <RAGBadge status="RED" label="Active Incident" />
        </div>
      </div>

      {/* Incident Overview */}
      <div className="card">
        <h2 className="panel-title">Incident Overview</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div>
            <span className="text-gray-500">Type:</span>{" "}
            <span className="font-medium">{inc.incident_type || "N/A"}</span>
          </div>
          <div>
            <span className="text-gray-500">DMA:</span>{" "}
            <span className="font-medium">{inc.dma_code || "N/A"}</span>
          </div>
          <div>
            <span className="text-gray-500">Started:</span>{" "}
            <span className="font-medium">
              {createdAt ? format(createdAt, "dd MMM yyyy HH:mm") : "N/A"}
            </span>
          </div>
          <div>
            <span className="text-gray-500">Status:</span>{" "}
            <span className="font-medium capitalize">{inc.status || "N/A"}</span>
          </div>
          <div className="md:col-span-2">
            <span className="text-gray-500">Description:</span>{" "}
            <span className="font-medium">{inc.description || "N/A"}</span>
          </div>
        </div>
      </div>

      {/* Actions Taken */}
      <div className="card">
        <h2 className="panel-title">Actions Taken</h2>
        {actionsTaken.length > 0 ? (
          <ul className="space-y-2 text-sm">
            {actionsTaken.map((a: any, i: number) => (
              <li key={i} className="flex items-start gap-2">
                <span className="mt-1 w-2 h-2 rounded-full bg-green-500 flex-shrink-0" />
                <div>
                  <span className="font-medium">{a.event_type || "Action"}</span>
                  <span className="text-gray-500 ml-2">{a.description}</span>
                  {a.event_time && (
                    <span className="text-gray-400 ml-2 text-xs">
                      {format(new Date(a.event_time), "HH:mm")}
                    </span>
                  )}
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <EmptyState title="No actions recorded" message="No completed actions in this incident yet." />
        )}
      </div>

      {/* Outstanding Actions */}
      <div className="card">
        <h2 className="panel-title">Outstanding Actions</h2>
        {outstanding.length > 0 ? (
          <ul className="space-y-2 text-sm">
            {outstanding.map((a: any, i: number) => (
              <li key={i} className="flex items-start gap-2">
                <span className="mt-1 w-2 h-2 rounded-full bg-amber-500 flex-shrink-0" />
                <div>
                  <span className="font-medium">{a.event_type || "Action"}</span>
                  <span className="text-gray-500 ml-2">{a.description}</span>
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <EmptyState title="No outstanding actions" message="All actions have been addressed." />
        )}
      </div>

      {/* Current Trajectory */}
      <div className="card">
        <h2 className="panel-title">Current Trajectory & Escalation Risk</h2>
        <div className="text-sm text-gray-600 space-y-2">
          <p>
            <span className="font-medium">Duration:</span> {elapsed}
          </p>
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

      {/* Acknowledge */}
      <div className="card flex items-center justify-between">
        <div>
          <h2 className="font-semibold text-water-800">Acknowledge Handover</h2>
          <p className="text-sm text-gray-500">Confirm you have reviewed this handover briefing.</p>
        </div>
        <button
          className={acknowledged ? "btn-secondary cursor-default" : "btn-primary"}
          onClick={() => !acknowledged && ackMutation.mutate()}
          disabled={acknowledged || ackMutation.isPending}
        >
          {acknowledged ? "Acknowledged" : ackMutation.isPending ? "Saving..." : "Acknowledge"}
        </button>
      </div>
    </div>
  );
}
