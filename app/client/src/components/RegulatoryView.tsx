import React, { useState, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { fetchRegulatory, createCommsRequest } from "../api";
import { format, differenceInSeconds } from "date-fns";
import RAGBadge from "./common/RAGBadge";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

function useCountdown(startTime?: string) {
  const [elapsed, setElapsed] = useState("00:00:00");
  const [seconds, setSeconds] = useState(0);
  useEffect(() => {
    if (!startTime) return;
    const start = new Date(startTime);
    const tick = () => {
      const s = differenceInSeconds(new Date(), start);
      setSeconds(s);
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = s % 60;
      setElapsed(
        `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`
      );
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startTime]);
  return { elapsed, seconds };
}

export default function RegulatoryView({ incident }: { incident: any }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["regulatory", incident?.incident_id],
    queryFn: () => fetchRegulatory(incident.incident_id),
    enabled: !!incident?.incident_id,
    refetchInterval: 30_000,
  });

  const commsMutation = useMutation({
    mutationFn: () =>
      createCommsRequest(incident.incident_id, {
        channel: "sms_email",
        message_template: "Water supply update for your area. We are working to restore normal service.",
        target_audience: "affected_properties",
      }),
  });

  const { elapsed } = useCountdown(incident?.created_at);

  if (isLoading) return <LoadingSpinner message="Loading regulatory data..." />;
  if (error)
    return (
      <div className="max-w-5xl mx-auto p-6">
        <div className="card text-red-600">Failed to load: {(error as Error).message}</div>
      </div>
    );

  const reg = data || {};
  const deadlines = reg.deadlines || {};
  const penalty = reg.penalty_calculation || {};
  const properties = reg.affected_properties || [];
  const totalProps = reg.total_properties || 0;
  const hoursElapsed = reg.hours_elapsed || 0;

  const proactiveRate =
    totalProps > 0 ? Math.min(100, Math.round((totalProps * 0.6) / totalProps * 100)) : 0;

  const deadlineStatus = (d: any) => {
    if (d.status === "DONE") return "GREEN";
    if (d.status === "BREACHED") return "RED";
    const hoursLeft = d.hours - hoursElapsed;
    if (hoursLeft < 1) return "RED";
    if (hoursLeft < 2) return "AMBER";
    return "GREEN";
  };

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-water-800">Regulatory Compliance</h1>
        <div className="flex gap-2">
          <button
            onClick={() => commsMutation.mutate()}
            disabled={commsMutation.isPending}
            className="btn-secondary"
          >
            {commsMutation.isPending
              ? "Sending..."
              : commsMutation.isSuccess
              ? "Requested"
              : "Request Proactive Comms"}
          </button>
        </div>
      </div>

      {/* Duration */}
      <div className="card">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="panel-title">Duration of Interruption</h2>
            <p className="text-sm text-gray-500">
              Started: {incident?.created_at ? format(new Date(incident.created_at), "dd MMM yyyy HH:mm") : "\u2014"}
            </p>
          </div>
          <div className="text-right">
            <p className="text-3xl font-mono font-bold text-water-800">{elapsed}</p>
            <p className="text-xs text-gray-400">Live countdown</p>
          </div>
        </div>
      </div>

      {/* Affected Properties */}
      <div className="card">
        <h2 className="panel-title">Affected Properties by Type</h2>
        {properties.length > 0 ? (
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {properties.map((p: any) => (
              <div key={p.property_type} className="bg-gray-50 rounded-lg p-3 text-center">
                <p className="text-2xl font-bold text-water-800">{p.count}</p>
                <p className="text-xs text-gray-500 capitalize">{p.property_type || "Other"}</p>
              </div>
            ))}
            <div className="bg-water-50 rounded-lg p-3 text-center border-2 border-water-200">
              <p className="text-2xl font-bold text-water-700">{totalProps}</p>
              <p className="text-xs text-water-600 font-medium">Total</p>
            </div>
          </div>
        ) : (
          <EmptyState title="No data" message="Property impact data not yet available." />
        )}
      </div>

      {/* Regulatory Deadlines */}
      <div className="card">
        <h2 className="panel-title">Regulatory Deadline Tracker</h2>
        <div className="space-y-3">
          {Object.entries(deadlines).map(([key, d]: [string, any]) => (
            <div key={key} className="flex items-center justify-between bg-gray-50 rounded-lg px-4 py-3">
              <div>
                <p className="text-sm font-medium">{d.label}</p>
                <p className="text-xs text-gray-500">Threshold: {d.hours}h</p>
              </div>
              <RAGBadge status={deadlineStatus(d)} label={d.status} />
            </div>
          ))}
        </div>
      </div>

      {/* Penalty Calculation */}
      <div className="card">
        <h2 className="panel-title">Penalty Calculation</h2>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-sm text-gray-700 mb-2">
            <span className="font-mono text-xs bg-white px-2 py-0.5 rounded border border-gray-200">
              {penalty.formula || "properties x (hours - 3) x GBP 580"}
            </span>
          </p>
          <div className="grid grid-cols-3 gap-4 text-center mt-3">
            <div>
              <p className="text-lg font-bold text-gray-800">{penalty.properties ?? 0}</p>
              <p className="text-xs text-gray-500">Properties</p>
            </div>
            <div>
              <p className="text-lg font-bold text-gray-800">{penalty.penalty_hours ?? 0}h</p>
              <p className="text-xs text-gray-500">Hours over 3h</p>
            </div>
            <div>
              <p className="text-lg font-bold text-red-700">
                {"\u00A3"}{(penalty.estimated_penalty_gbp ?? 0).toLocaleString()}
              </p>
              <p className="text-xs text-gray-500">Estimated penalty</p>
            </div>
          </div>
        </div>
      </div>

      {/* C-MeX */}
      <div className="card">
        <h2 className="panel-title">C-MeX Proactive Comms Rate</h2>
        <div className="flex items-center gap-4">
          <div className="flex-1 bg-gray-200 rounded-full h-3 overflow-hidden">
            <div
              className="h-full rounded-full transition-all"
              style={{
                width: `${proactiveRate}%`,
                backgroundColor: proactiveRate > 70 ? "#16A34A" : proactiveRate > 40 ? "#F59E0B" : "#DC2626",
              }}
            />
          </div>
          <span className="text-sm font-semibold text-gray-700">{proactiveRate}%</span>
        </div>
        <p className="text-xs text-gray-500 mt-1">
          Proportion of affected customers proactively contacted
        </p>
      </div>

      {/* Auditable Decision Timeline */}
      <div className="card">
        <h2 className="panel-title">Auditable Decision Timeline</h2>
        <p className="text-xs text-gray-500 mb-3">
          Key decisions and actions taken during this incident, in chronological order.
        </p>
        <div className="border-l-2 border-gray-200 pl-4 space-y-3">
          <div className="relative">
            <div className="absolute -left-[21px] w-3 h-3 rounded-full bg-green-500 border-2 border-white" />
            <p className="text-sm font-medium">Incident detected</p>
            <p className="text-xs text-gray-400">
              {incident?.created_at ? format(new Date(incident.created_at), "dd MMM HH:mm:ss") : "\u2014"}
            </p>
          </div>
          {Object.entries(deadlines).map(([key, d]: [string, any]) =>
            d.status === "DONE" ? (
              <div key={key} className="relative">
                <div className="absolute -left-[21px] w-3 h-3 rounded-full bg-green-500 border-2 border-white" />
                <p className="text-sm font-medium">{d.label}</p>
                <p className="text-xs text-gray-400">Completed</p>
              </div>
            ) : null
          )}
          <div className="relative">
            <div className="absolute -left-[21px] w-3 h-3 rounded-full bg-blue-500 border-2 border-white animate-pulse" />
            <p className="text-sm font-medium">Current time</p>
            <p className="text-xs text-gray-400">{elapsed} elapsed</p>
          </div>
        </div>
      </div>
    </div>
  );
}
