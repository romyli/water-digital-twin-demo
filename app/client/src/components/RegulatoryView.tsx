import { useMutation } from "@tanstack/react-query";
import { useQuery } from "@tanstack/react-query";
import { fetchRegulatory, createCommsRequest } from "../api";
import { format } from "date-fns";
import { useCountdown } from "../hooks/useCountdown";
import { humanize } from "../utils/format";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

/* ---------- C-MeX Ring Gauge ---------- */
function CmexRing({ percentage, greenThreshold = 70, amberThreshold = 40 }: { percentage: number; greenThreshold?: number; amberThreshold?: number }) {
  const pct = Math.max(0, Math.min(100, percentage));
  const color = pct > greenThreshold ? "#16A34A" : pct > amberThreshold ? "#F59E0B" : "#DC2626";
  const circumference = 2 * Math.PI * 54;
  const offset = circumference - (pct / 100) * circumference;
  return (
    <div className="relative w-32 h-32 mx-auto">
      <svg viewBox="0 0 120 120" className="w-full h-full -rotate-90">
        <circle cx="60" cy="60" r="54" fill="none" stroke="#e5e7eb" strokeWidth="10" />
        <circle
          cx="60"
          cy="60"
          r="54"
          fill="none"
          stroke={color}
          strokeWidth="10"
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          className="transition-all duration-500"
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-2xl font-bold tabular-nums" style={{ color }}>{pct}%</span>
      </div>
    </div>
  );
}

/* ---------- Deadline Progress Bar ---------- */
function DeadlineBar({
  deadline,
  hoursElapsed,
}: {
  deadline: any;
  hoursElapsed: number;
}) {
  const isDone = deadline.status === "DONE";
  const isBreached = deadline.status === "BREACHED";
  const pctUsed = Math.min(100, (hoursElapsed / deadline.hours) * 100);
  const hoursLeft = Math.max(0, deadline.hours - hoursElapsed);

  let barColor = "bg-green-500";
  if (pctUsed > 90 || isBreached) barColor = "bg-red-500";
  else if (pctUsed > 70) barColor = "bg-amber-500";

  const wrapperClass = isBreached
    ? "bg-red-50 border-2 border-red-300 rounded-lg p-4"
    : "bg-gray-50 rounded-lg p-4";

  return (
    <div className={wrapperClass}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          {isDone ? (
            <svg className="w-4 h-4 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
            </svg>
          ) : isBreached ? (
            <svg className="w-4 h-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          ) : pctUsed > 70 ? (
            <svg className="w-4 h-4 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
            </svg>
          ) : null}
          <p className="text-sm font-medium">{deadline.label}</p>
        </div>
        <div className="text-right">
          {isBreached ? (
            <span className="inline-flex items-center gap-1.5 bg-red-100 text-red-700 px-2 py-0.5 rounded-full text-xs font-bold">
              BREACHED — {deadline.hours}h
            </span>
          ) : isDone ? (
            <span className="text-green-600 font-medium text-sm">Complete</span>
          ) : (
            <span className="text-lg font-bold tabular-nums text-gray-800">
              {hoursLeft.toFixed(1)}h remaining
            </span>
          )}
        </div>
      </div>
      <div className="w-full h-2.5 bg-gray-200 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${barColor}`}
          style={{ width: `${isDone ? 100 : pctUsed}%` }}
        />
      </div>
      <p className="text-xs text-gray-400 mt-1">Threshold: {deadline.hours}h</p>
    </div>
  );
}

export default function RegulatoryView({ incident, timeOffset = 0 }: { incident: any; timeOffset?: number }) {
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

  const { elapsed } = useCountdown(incident?.created_at || incident?.start_timestamp, timeOffset);

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
  const commsRequested = reg.proactive_comms_requested || false;
  const proactiveRate = commsRequested ? 100 : 0;
  const regRules = reg.rules || {};
  const penaltyRate = regRules.penalty_rate ?? 580;
  const gracePeriod = regRules.grace_period ?? 3;
  const cmexGreen = regRules.cmex_green ?? 70;
  const cmexAmber = regRules.cmex_amber ?? 40;

  const incidentStart = incident?.created_at || incident?.start_timestamp;
  const startDate = incidentStart ? new Date(incidentStart) : null;

  // Build auditable timeline
  const timelineEntries: {
    label: string;
    attribution?: string;
    timestamp?: string;
    done: boolean;
    elapsedFromPrev?: string;
  }[] = [];

  timelineEntries.push({
    label: "Incident detected",
    attribution: "Detected by SCADA",
    timestamp: startDate ? format(startDate, "dd MMM HH:mm:ss") : undefined,
    done: true,
  });

  let prevTimeSec = startDate ? startDate.getTime() / 1000 : 0;

  Object.entries(deadlines).forEach(([_key, d]: [string, any]) => {
    if (d.status === "DONE" || d.status === "BREACHED") {
      const entryTimeSec = prevTimeSec + d.hours * 3600;
      const diffMin = Math.round((entryTimeSec - prevTimeSec) / 60);
      timelineEntries.push({
        label: d.label,
        attribution: d.status === "DONE" ? "Completed" : "BREACHED",
        done: true,
        elapsedFromPrev: diffMin > 0 ? `+${diffMin} min` : undefined,
      });
      prevTimeSec = entryTimeSec;
    }
  });

  // Pending deadlines
  Object.entries(deadlines).forEach(([_key, d]: [string, any]) => {
    if (d.status !== "DONE" && d.status !== "BREACHED") {
      timelineEntries.push({
        label: d.label,
        attribution: `Deadline: ${d.hours}h`,
        done: false,
      });
    }
  });

  timelineEntries.push({
    label: "Current time",
    attribution: `${elapsed} elapsed`,
    done: true,
  });

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-water-800">Regulatory Compliance</h1>
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

      {/* Duration */}
      <div className="card">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="panel-title">Duration of Interruption</h2>
            <p className="text-sm text-gray-500">
              Started: {startDate ? format(startDate, "dd MMM yyyy HH:mm") : "—"}
            </p>
          </div>
          <div className="text-right">
            <p className="text-3xl font-mono font-bold text-water-800 tabular-nums">{elapsed}</p>
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
                <p className="text-2xl font-bold text-water-800 tabular-nums">{p.count}</p>
                <p className="text-xs text-gray-500">{humanize(p.property_type) || "Other"}</p>
              </div>
            ))}
            <div className="bg-water-50 rounded-lg p-3 text-center border-2 border-water-200">
              <p className="text-2xl font-bold text-water-700 tabular-nums">{totalProps}</p>
              <p className="text-xs text-water-600 font-medium">Total</p>
            </div>
          </div>
        ) : (
          <EmptyState title="No data" message="Property impact data not yet available." />
        )}
      </div>

      {/* Regulatory Deadline Progress Bars */}
      <div className="card">
        <h2 className="panel-title">Regulatory Deadline Tracker</h2>
        <div className="space-y-3">
          {Object.entries(deadlines).map(([key, d]: [string, any]) => (
            <DeadlineBar key={key} deadline={d} hoursElapsed={hoursElapsed} />
          ))}
        </div>
      </div>

      {/* Penalty Calculation — Hero */}
      <div className="card border-l-4 border-l-red-500">
        <h2 className="panel-title">Projected Penalty</h2>
        <p className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1 mb-4 inline-block font-medium">
          PROJECTED — assumes no remedial action
        </p>
        <div className="text-center mb-4">
          <p className="text-4xl font-bold text-red-600 tabular-nums">
            {"\u00A3"}{(penalty.estimated_penalty_gbp ?? 0).toLocaleString()}
          </p>
        </div>
        {/* Visual formula */}
        <div className="flex items-center justify-center gap-2 flex-wrap">
          <div className="bg-white rounded-lg px-3 py-2 border shadow-sm text-center">
            <p className="text-lg font-bold text-gray-800 tabular-nums">{penalty.properties ?? 0}</p>
            <p className="text-xs text-gray-500">Properties</p>
          </div>
          <span className="text-gray-400 font-bold text-lg">&times;</span>
          <div className="bg-white rounded-lg px-3 py-2 border shadow-sm text-center">
            <p className="text-lg font-bold text-gray-800 tabular-nums">{penalty.penalty_hours ?? 0}h</p>
            <p className="text-xs text-gray-500">Hours over {gracePeriod}h</p>
          </div>
          <span className="text-gray-400 font-bold text-lg">&times;</span>
          <div className="bg-white rounded-lg px-3 py-2 border shadow-sm text-center">
            <p className="text-lg font-bold text-gray-800">{"\u00A3"}{penaltyRate}</p>
            <p className="text-xs text-gray-500">Per property/hr</p>
          </div>
        </div>
      </div>

      {/* C-MeX Ring Gauge */}
      <div className="card">
        <h2 className="panel-title">C-MeX Proactive Comms Rate</h2>
        <CmexRing percentage={proactiveRate} greenThreshold={cmexGreen} amberThreshold={cmexAmber} />
        <p className="text-xs text-gray-500 mt-3 text-center">
          Proportion of affected customers proactively contacted
        </p>
        <p className="text-xs text-gray-400 text-center mt-1">
          Target: &gt;85% for upper quartile C-MeX
        </p>
      </div>

      {/* Auditable Decision Timeline */}
      <div className="card">
        <h2 className="panel-title">Auditable Decision Timeline</h2>
        <p className="text-xs text-gray-500 mb-3">
          Key decisions and actions taken during this incident, in chronological order.
        </p>
        <div className="border-l-2 border-gray-200 pl-6 space-y-4">
          {timelineEntries.map((entry, i) => {
            const isLast = i === timelineEntries.length - 1;
            const isPending = !entry.done;
            return (
              <div key={i} className="relative">
                {/* Elapsed connector */}
                {entry.elapsedFromPrev && (
                  <div className="absolute -left-[33px] -top-3 text-[10px] text-gray-400 bg-white px-1">
                    {entry.elapsedFromPrev}
                  </div>
                )}
                {/* Node */}
                <div
                  className={`absolute -left-[33px] w-3.5 h-3.5 rounded-full border-2 border-white ${
                    isPending
                      ? "border-dashed border-gray-300 bg-white ring-2 ring-gray-300"
                      : isLast
                      ? "bg-blue-500 ring-2 ring-blue-500 animate-pulse"
                      : "bg-green-500 ring-2 ring-green-500"
                  }`}
                />
                <div>
                  <p className={`text-sm font-medium ${isPending ? "text-gray-400" : "text-gray-900"}`}>
                    {entry.label}
                  </p>
                  {entry.attribution && (
                    <p className="text-xs text-gray-400">{entry.attribution}</p>
                  )}
                  {entry.timestamp && (
                    <p className="text-xs text-gray-400">{entry.timestamp}</p>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
