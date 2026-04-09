import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchSensorTelemetry,
  fetchSensorAnomalies,
  fetchDMARagHistory,
  fetchPlaybook,
  savePlaybookActions,
} from "../api";
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
  ReferenceArea,
  ReferenceLine,
} from "recharts";
import { format } from "date-fns";
import RAGBadge from "./common/RAGBadge";
import TimelineStrip from "./common/TimelineStrip";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

export default function AssetDetail({
  sensorId,
  dmaCode,
  activeIncident,
  firstComplaintTime,
}: {
  sensorId: string;
  dmaCode: string;
  activeIncident: any;
  firstComplaintTime?: string;
}) {
  const queryClient = useQueryClient();
  const [decisions, setDecisions] = useState<Record<string, string>>({});

  const { data: telemetryData, isLoading: loadingTelemetry } = useQuery({
    queryKey: ["telemetry", sensorId],
    queryFn: () => fetchSensorTelemetry(sensorId),
    enabled: !!sensorId,
  });

  const { data: anomalyData } = useQuery({
    queryKey: ["anomalies", sensorId],
    queryFn: () => fetchSensorAnomalies(sensorId),
    enabled: !!sensorId,
  });

  const { data: ragData } = useQuery({
    queryKey: ["dmaRag", dmaCode],
    queryFn: () => fetchDMARagHistory(dmaCode),
    enabled: !!dmaCode,
  });

  const incidentType = activeIncident?.incident_type || "low_pressure";
  const { data: playbookData } = useQuery({
    queryKey: ["playbook", incidentType],
    queryFn: () => fetchPlaybook(incidentType),
    enabled: !!incidentType,
  });

  const telemetry = telemetryData?.telemetry || [];
  const anomalies = anomalyData?.anomalies || [];
  const ragHistory = ragData?.rag_history || [];
  const playbookSteps = playbookData?.steps || [];

  const chartData = useMemo(() => {
    return telemetry.map((t: any) => ({
      time: t.ts ? format(new Date(t.ts), "HH:mm") : "",
      pressure: t.pressure != null ? Number(t.pressure) : null,
      flow: t.flow != null ? Number(t.flow) : null,
    }));
  }, [telemetry]);

  // Oldest anomaly in the window (API returns DESC order, so last = earliest)
  const firstAnomaly = anomalies.length > 0 ? anomalies[anomalies.length - 1] : null;
  const anomalyLeadMin = useMemo(() => {
    if (!firstAnomaly?.scored_at || !firstComplaintTime) return null;
    const diff =
      (new Date(firstComplaintTime).getTime() - new Date(firstAnomaly.scored_at).getTime()) / 60000;
    return diff > 0 ? Math.round(diff) : null;
  }, [firstAnomaly, firstComplaintTime]);

  // Chart annotation times
  const anomalyChartTime = firstAnomaly?.scored_at
    ? format(new Date(firstAnomaly.scored_at), "HH:mm")
    : null;
  const complaintChartTime = firstComplaintTime
    ? format(new Date(firstComplaintTime), "HH:mm")
    : null;

  const saveMutation = useMutation({
    mutationFn: (actions: any[]) => savePlaybookActions(activeIncident?.incident_id, actions),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["playbook"] }),
  });

  const handleDecision = (actionId: string, decision: string) => {
    setDecisions((prev) => ({ ...prev, [actionId]: decision }));
  };

  const handleSavePlaybook = () => {
    const actions = Object.entries(decisions).map(([action_id, decision]) => ({
      action_id,
      decision,
    }));
    if (actions.length > 0) saveMutation.mutate(actions);
  };

  const decidedCount = Object.keys(decisions).length;
  const totalSteps = playbookSteps.length;

  if (loadingTelemetry) return <LoadingSpinner message="Loading sensor data..." />;

  return (
    <div className="space-y-4">
      <h3 className="text-base font-semibold text-water-800">{sensorId}</h3>

      {/* Anomaly Hero Callout */}
      {firstAnomaly && anomalyLeadMin != null && (
        <div className="bg-gradient-to-r from-red-600 to-red-700 text-white rounded-xl p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-3xl font-bold tabular-nums">{anomalyLeadMin} min</p>
              <p className="text-red-100 text-sm mt-1">
                AI detected this anomaly {anomalyLeadMin} minutes before the first customer complaint
              </p>
            </div>
            {firstAnomaly.score != null && (
              <div className="text-right">
                <p className="text-xs text-red-200">Confidence</p>
                <div className="w-24 h-2 bg-red-800 rounded-full mt-1 overflow-hidden">
                  <div
                    className="h-full bg-white rounded-full"
                    style={{ width: `${Number(firstAnomaly.score) * 100}%` }}
                  />
                </div>
                <p className="text-xs text-red-200 mt-0.5 tabular-nums">
                  {(Number(firstAnomaly.score) * 100).toFixed(0)}%
                </p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Fallback: simple anomaly notice when no lead time */}
      {firstAnomaly && anomalyLeadMin == null && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm">
          <div className="flex items-center gap-2">
            <RAGBadge status="RED" label="Anomaly Detected" pulse />
          </div>
          <p className="text-red-700 mt-1">
            Anomaly detected at{" "}
            {firstAnomaly.scored_at ? format(new Date(firstAnomaly.scored_at), "HH:mm") : "—"}
          </p>
        </div>
      )}

      {/* Chart with annotations */}
      {chartData.length > 0 ? (
        <div className="bg-gray-50 rounded-lg p-3">
          <p className="text-xs font-medium text-gray-500 mb-2">Telemetry (24h)</p>
          <ResponsiveContainer width="100%" height={280}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis
                yAxisId="left"
                tick={{ fontSize: 10 }}
                label={{ value: "Pressure (m)", angle: -90, position: "insideLeft", style: { fontSize: 10 } }}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fontSize: 10 }}
                label={{ value: "Flow (l/s)", angle: 90, position: "insideRight", style: { fontSize: 10 } }}
              />
              <Tooltip contentStyle={{ fontSize: 11 }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />

              {/* Low pressure danger zone */}
              <ReferenceArea
                yAxisId="left"
                y1={0}
                y2={10}
                fill="rgba(220,38,38,0.08)"
                fillOpacity={1}
              />

              {/* Anomaly detection line */}
              {anomalyChartTime && (
                <ReferenceLine
                  x={anomalyChartTime}
                  stroke="#DC2626"
                  strokeDasharray="5 5"
                  strokeWidth={1.5}
                  label={{ value: "Anomaly detected", position: "top", fill: "#DC2626", fontSize: 10 }}
                />
              )}

              {/* First complaint line */}
              {complaintChartTime && (
                <ReferenceLine
                  x={complaintChartTime}
                  stroke="#F59E0B"
                  strokeDasharray="5 5"
                  strokeWidth={1.5}
                  label={{ value: "First complaint", position: "top", fill: "#F59E0B", fontSize: 10 }}
                />
              )}

              <Line
                yAxisId="left"
                type="monotone"
                dataKey="pressure"
                stroke="#2563EB"
                strokeWidth={2}
                dot={false}
                name="Pressure"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="flow"
                stroke="#16A34A"
                strokeWidth={2}
                dot={false}
                name="Flow"
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <EmptyState title="No telemetry" message="No telemetry data available for this sensor." />
      )}

      <div>
        <p className="text-xs font-medium text-gray-500 mb-1">RAG Timeline</p>
        <TimelineStrip history={ragHistory} />
      </div>

      {/* Response Playbook with numbered steps and progress */}
      {playbookSteps.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs font-medium text-gray-500">Response Playbook</p>
            <span className="text-xs text-gray-400 tabular-nums">
              {decidedCount} of {totalSteps} steps decided
            </span>
          </div>
          {/* Progress bar */}
          <div className="w-full h-1.5 bg-gray-200 rounded-full mb-3 overflow-hidden">
            <div
              className="h-full bg-water-600 rounded-full transition-all"
              style={{ width: totalSteps > 0 ? `${(decidedCount / totalSteps) * 100}%` : "0%" }}
            />
          </div>
          <div className="space-y-2">
            {playbookSteps.map((step: any, i: number) => {
              const actionId = step.action_id || step.step_id || `step-${i}`;
              const current = decisions[actionId];
              const stepNum = step.step_order ?? i + 1;
              return (
                <div key={actionId} className="bg-gray-50 rounded-lg p-3 text-sm flex gap-3">
                  {/* Step number circle */}
                  <div className="flex-shrink-0">
                    {current === "Accept" ? (
                      <div className="w-7 h-7 rounded-full bg-green-500 text-white flex items-center justify-center">
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                        </svg>
                      </div>
                    ) : current === "Defer" ? (
                      <div className="w-7 h-7 rounded-full bg-amber-500 text-white flex items-center justify-center">
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5" />
                        </svg>
                      </div>
                    ) : current === "Not Applicable" ? (
                      <div className="w-7 h-7 rounded-full bg-gray-400 text-white flex items-center justify-center text-xs font-bold">
                        —
                      </div>
                    ) : (
                      <div className="w-7 h-7 rounded-full border-2 border-gray-300 text-gray-400 flex items-center justify-center text-xs font-bold tabular-nums">
                        {stepNum}
                      </div>
                    )}
                  </div>
                  <div className="flex-1">
                    <span className="font-medium">
                      {step.action || step.description}
                    </span>
                    <div className="flex gap-1.5 mt-2">
                      {["Accept", "Defer", "Not Applicable"].map((opt) => (
                        <button
                          key={opt}
                          onClick={() => handleDecision(actionId, opt)}
                          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                            current === opt
                              ? opt === "Accept"
                                ? "bg-green-600 text-white"
                                : opt === "Defer"
                                ? "bg-amber-500 text-white"
                                : "bg-gray-500 text-white"
                              : "bg-white border border-gray-200 text-gray-600 hover:bg-gray-50"
                          }`}
                        >
                          {opt === "Not Applicable" ? "N/A" : opt}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              );
            })}
            <button
              onClick={handleSavePlaybook}
              disabled={decidedCount === 0 || saveMutation.isPending}
              className="btn-primary w-full mt-2"
            >
              {saveMutation.isPending
                ? "Saving..."
                : saveMutation.isSuccess
                ? "Saved"
                : "Save Decisions"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
